use std::pin::Pin;

use hermes_broker::router::RouterCmd;
use hermes_broker::slot::{Delivery, SubHandle, SubId};
use hermes_proto::{
    PublishAck, PublishRequest, SubscribeRequest, SubscribeResponse, broker_server::Broker,
};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Streaming};

pub struct BrokerService {
    router_tx: mpsc::Sender<RouterCmd>,
}

impl BrokerService {
    pub fn new(router_tx: mpsc::Sender<RouterCmd>) -> Self {
        Self { router_tx }
    }
}

type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl Broker for BrokerService {
    type SubscribeStream = GrpcStream<SubscribeResponse>;

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
                Err(_) => break,
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
                return Err(tonic::Status::internal("router unavailable"));
            }

            count += 1;
        }

        Ok(Response::new(PublishAck {
            total_published: count,
        }))
    }

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let router_tx = self.router_tx.clone();
        let (resp_tx, resp_rx) = mpsc::channel::<Result<SubscribeResponse, tonic::Status>>(256);

        tokio::spawn(async move {
            let mut active_subs: Vec<SubId> = Vec::new();

            while let Some(result) = inbound.next().await {
                let msg = match result {
                    Ok(m) => m,
                    Err(_) => break,
                };

                let Some(sub) = msg.sub else { continue };

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
                                            _,
                                        )) => continue,
                                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        SubHandle::QueueMember { mut rx, .. } => {
                            tokio::spawn(async move {
                                while let Some(delivery) = rx.recv().await {
                                    if resp_tx
                                        .send(Ok(delivery_to_response(delivery)))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            });
                        }
                    }
                }
            }

            // Client disconnected — clean up all subscriptions.
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
