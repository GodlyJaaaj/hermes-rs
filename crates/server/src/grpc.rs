use std::pin::Pin;

use hermes_broker::router::RouterCmd;
use hermes_broker::slot::{Delivery, SubHandle, SubId};
use hermes_proto::{
    PublishAck, PublishRequest, SubscribeRequest, SubscribeResponse, broker_server::Broker,
};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Streaming};
use tracing::{debug, info, warn};

/// gRPC implementation of the Hermes [`Broker`] service.
///
/// Bridges incoming gRPC streams to the core router via [`RouterCmd`] messages.
pub struct BrokerService {
    router_tx: mpsc::Sender<RouterCmd>,
}

impl BrokerService {
    /// Create a new broker service backed by the given router command channel.
    pub fn new(router_tx: mpsc::Sender<RouterCmd>) -> Self {
        Self { router_tx }
    }
}

type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl Broker for BrokerService {
    async fn publish(
        &self,
        request: Request<Streaming<PublishRequest>>,
    ) -> Result<Response<PublishAck>, tonic::Status> {
        let mut inbound = request.into_inner();
        let router_tx = self.router_tx.clone();
        let mut count: u64 = 0;

        while let Some(result) = inbound.next().await {
            let msg = match result {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        error = %e,
                        published_so_far = count,
                        "publish stream error"
                    );
                    break;
                }
            };

            let cmd = RouterCmd::Publish {
                subject: msg.subject.into(),
                payload: msg.payload.into(),
                reply_to: if msg.reply_to.is_empty() {
                    None
                } else {
                    Some(msg.reply_to.into())
                },
            };

            if router_tx.send(cmd).await.is_err() {
                warn!(
                    published_so_far = count,
                    "router unavailable during publish"
                );
                return Err(tonic::Status::internal("router unavailable"));
            }

            count += 1;
        }

        info!(total_published = count, "publish stream completed");

        Ok(Response::new(PublishAck {
            total_published: count,
        }))
    }

    type SubscribeStream = GrpcStream<SubscribeResponse>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let router_tx = self.router_tx.clone();
        let (resp_tx, resp_rx) = mpsc::channel::<Result<SubscribeResponse, tonic::Status>>(256);

        info!("new subscribe stream opened");

        tokio::spawn(async move {
            let mut active_subs: Vec<SubId> = Vec::new();

            while let Some(result) = inbound.next().await {
                let msg = match result {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, "subscribe stream error");
                        break;
                    }
                };

                let Some(sub) = msg.sub else { continue };

                debug!(
                    subject = %sub.subject,
                    queue_group = %sub.queue_group,
                    "processing subscribe request"
                );

                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let cmd = RouterCmd::Subscribe {
                    subject: sub.subject.clone().into(),
                    queue_group: if sub.queue_group.is_empty() {
                        None
                    } else {
                        Some(sub.queue_group.into())
                    },
                    reply: reply_tx,
                };

                if router_tx.send(cmd).await.is_err() {
                    warn!("router unavailable during subscribe");
                    break;
                }

                if let Ok(handle) = reply_rx.await {
                    let sub_id = match &handle {
                        SubHandle::Fanout { sub_id, .. } => *sub_id,
                        SubHandle::QueueMember { sub_id, .. } => *sub_id,
                    };
                    active_subs.push(sub_id);

                    let resp_tx = resp_tx.clone();
                    match handle {
                        SubHandle::Fanout { mut rx, .. } => {
                            tokio::spawn(async move {
                                loop {
                                    match rx.recv().await {
                                        Ok(delivery) => {
                                            if resp_tx
                                                .send(Ok(delivery_to_response(delivery)))
                                                .await
                                                .is_err()
                                            {
                                                break;
                                            }
                                        }
                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(
                                            n,
                                        )) => {
                                            warn!(
                                                sub_id = sub_id.0,
                                                skipped = n,
                                                "fanout subscriber lagged, messages lost"
                                            );
                                            continue;
                                        }
                                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                            break;
                                        }
                                    }
                                }
                                debug!(sub_id = sub_id.0, "fanout forwarding task ended");
                            });
                        }
                        SubHandle::QueueMember { rx, .. } => {
                            tokio::spawn(async move {
                                while let Ok(delivery) = rx.recv().await {
                                    if resp_tx
                                        .send(Ok(delivery_to_response(delivery)))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                debug!(sub_id = sub_id.0, "queue member forwarding task ended");
                            });
                        }
                    }
                }
            }

            // Client disconnected — clean up all subscriptions.
            info!(
                active_subscriptions = active_subs.len(),
                "subscribe stream closed, cleaning up"
            );
            for sub_id in active_subs {
                let _ = router_tx.send(RouterCmd::Disconnect { sub_id }).await;
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(resp_rx);
        Ok(Response::new(Box::pin(stream)))
    }
}

fn delivery_to_response(d: Delivery) -> SubscribeResponse {
    SubscribeResponse {
        subject: d.subject.into(),
        payload: d.payload.to_vec(),
        sequence: d.sequence,
        reply_to: d.reply_to.map(|s| s.into()).unwrap_or_default(),
    }
}
