use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use hermes_broker::router::RouterCmd;
use hermes_broker::slot::{Delivery, SessionId};
use hermes_proto::{
    Message, PublishAck, PublishRequest, SubscribeRequest, SubscribeResponse,
    broker_server::Broker,
};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Streaming};
use tracing::{debug, info, warn};

use crate::delivery_rx::{RxOutcome, TryRxOutcome, split_handle};

/// Max deliveries accumulated into a single `SubscribeResponse` frame.
/// Caps worst-case frame size so one sub can't monopolize the h2 write loop.
const BATCH_MAX: usize = 32;

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
                        forward_deliveries(sub_id.0, &mut rx, resp_tx).await;
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

fn delivery_to_message(d: &Delivery) -> Message {
    Message {
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

/// Drain a subscriber's delivery channel into the outbound gRPC response
/// channel, opportunistically batching deliveries that are already ready
/// into one `SubscribeResponse` frame. Never waits on a timer — the block
/// only happens on the first delivery; subsequent ones are drained via
/// `try_recv` until the channel reports `Empty`, then the batch flushes.
/// Under burst load this collapses N deliveries into 1 frame (fewer h2
/// lock acquisitions); under single-message load there is zero added
/// latency.
async fn forward_deliveries(
    sub_id: u64,
    rx: &mut crate::delivery_rx::DeliveryRx,
    resp_tx: mpsc::Sender<Result<SubscribeResponse, tonic::Status>>,
) {
    let mut batch: Vec<Message> = Vec::with_capacity(BATCH_MAX);

    loop {
        // Block on the first delivery of this round.
        match rx.recv().await {
            RxOutcome::Got(d) => batch.push(delivery_to_message(&d)),
            RxOutcome::Lagged(n) => {
                warn!(sub_id, skipped = n, "fanout subscriber lagged, messages lost");
                continue;
            }
            RxOutcome::Closed => return,
        }

        // Drain anything already ready, up to the batch cap.
        let mut closed = false;
        while batch.len() < BATCH_MAX {
            match rx.try_recv() {
                TryRxOutcome::Got(d) => batch.push(delivery_to_message(&d)),
                TryRxOutcome::Lagged(n) => {
                    warn!(sub_id, skipped = n, "fanout subscriber lagged, messages lost");
                }
                TryRxOutcome::Empty => break,
                TryRxOutcome::Closed => {
                    closed = true;
                    break;
                }
            }
        }

        let messages = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_MAX));
        if resp_tx
            .send(Ok(SubscribeResponse { messages }))
            .await
            .is_err()
        {
            return;
        }

        if closed {
            return;
        }
    }
}
