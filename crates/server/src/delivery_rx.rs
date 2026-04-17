//! Delivery receiver abstraction over fanout (tokio broadcast) and
//! queue-group (kanal MPMC). Centralizes the slight divergence between the
//! two primitives so the gRPC subscribe task can drive a single recv loop.

use std::sync::Arc;

use hermes_broker::slot::{Delivery, SubHandle, SubId};
use tokio::sync::broadcast;

/// Either end of a subscription's delivery channel.
pub enum DeliveryRx {
    Fanout(broadcast::Receiver<Arc<Delivery>>),
    Queue(kanal::AsyncReceiver<Arc<Delivery>>),
}

/// Normalized outcome of a single `recv().await`. `Lagged` only comes from
/// broadcast (the kanal side has no overflow mode: it back-pressures).
pub enum RxOutcome {
    Got(Arc<Delivery>),
    Lagged(u64),
    Closed,
}

impl DeliveryRx {
    pub async fn recv(&mut self) -> RxOutcome {
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

/// Split a `SubHandle` into its `SubId` and a unified [`DeliveryRx`].
pub fn split_handle(handle: SubHandle) -> (SubId, DeliveryRx) {
    match handle {
        SubHandle::Fanout { sub_id, rx } => (sub_id, DeliveryRx::Fanout(rx)),
        SubHandle::QueueMember { sub_id, rx } => (sub_id, DeliveryRx::Queue(rx)),
    }
}
