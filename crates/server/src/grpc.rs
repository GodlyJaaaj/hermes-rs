use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hermes_broker::router::RouterCmd;
use hermes_broker::slot::{Delivery, SessionId, SubHandle, SubId};
use hermes_proto::{
    PublishAck, PublishRequest, SubscribeRequest, SubscribeResponse, broker_server::Broker,
};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Streaming};
use tracing::{debug, info, warn};

/// Delivery receiver abstracted over fanout (broadcast) and queue-group (kanal MPMC).
/// Centralizes the slight divergence between `tokio::broadcast::Receiver` (has a
/// `Lagged` variant on overflow) and `kanal::AsyncReceiver` (closed = error).
enum DeliveryRx {
    Fanout(broadcast::Receiver<Arc<Delivery>>),
    Queue(kanal::AsyncReceiver<Arc<Delivery>>),
}

enum RxOutcome {
    Got(Arc<Delivery>),
    Lagged(u64),
    Closed,
}

impl DeliveryRx {
    async fn recv(&mut self) -> RxOutcome {
        match self {
            DeliveryRx::Fanout(rx) => match rx.recv().await {
                Ok(d) => RxOutcome::Got(d),
                Err(broadcast::error::RecvError::Lagged(n)) => RxOutcome::Lagged(n),
                Err(broadcast::error::RecvError::Closed) => RxOutcome::Closed,
            },
            DeliveryRx::Queue(rx) => match rx.recv().await {
                Ok(d) => RxOutcome::Got(d),
                Err(_) => RxOutcome::Closed,
            },
        }
    }
}

fn split_handle(handle: SubHandle) -> (SubId, DeliveryRx) {
    match handle {
        SubHandle::Fanout { sub_id, rx } => (sub_id, DeliveryRx::Fanout(rx)),
        SubHandle::QueueMember { sub_id, rx } => (sub_id, DeliveryRx::Queue(rx)),
    }
}

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

        // One session per subscribe stream. When the stream closes, a single
        // SessionEnd tells the router to drop every sub registered under it.
        static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
        let session_id = SessionId(NEXT_SESSION.fetch_add(1, Ordering::Relaxed));

        info!(session_id = session_id.0, "new subscribe stream opened");

        tokio::spawn(async move {
            let mut sub_count: usize = 0;

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
                    session_id = session_id.0,
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
                    session_id,
                    reply: reply_tx,
                };

                if router_tx.send(cmd).await.is_err() {
                    warn!("router unavailable during subscribe");
                    break;
                }

                if let Ok(handle) = reply_rx.await {
                    let (sub_id, mut rx) = split_handle(handle);
                    sub_count += 1;

                    let resp_tx = resp_tx.clone();
                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                RxOutcome::Got(delivery) => {
                                    if resp_tx
                                        .send(Ok(delivery_to_response(&delivery)))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                RxOutcome::Lagged(n) => {
                                    warn!(
                                        sub_id = sub_id.0,
                                        skipped = n,
                                        "fanout subscriber lagged, messages lost"
                                    );
                                }
                                RxOutcome::Closed => break,
                            }
                        }
                        debug!(sub_id = sub_id.0, "forwarding task ended");
                    });
                }
            }

            info!(
                session_id = session_id.0,
                sub_count,
                "subscribe stream closed, cleaning up"
            );
            let _ = router_tx
                .send(RouterCmd::SessionEnd { session_id })
                .await;
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(resp_rx);
        Ok(Response::new(Box::pin(stream)))
    }
}

fn delivery_to_response(d: &Delivery) -> SubscribeResponse {
    SubscribeResponse {
        subject: d.subject.to_string(),
        payload: d.payload.to_vec(),
        sequence: d.sequence,
        reply_to: d
            .reply_to
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_default(),
    }
}
