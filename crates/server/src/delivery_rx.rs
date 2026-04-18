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

/// Non-blocking outcome: `Empty` means nothing is immediately ready, used
/// by the batching forwarder to stop draining without yielding.
pub enum TryRxOutcome {
    Got(Arc<Delivery>),
    Lagged(u64),
    Empty,
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

    /// Non-blocking drain step for the batching forwarder. Returns
    /// `Empty` immediately if no delivery is ready — lets us batch
    /// opportunistically without ever waiting on a timer.
    pub fn try_recv(&mut self) -> TryRxOutcome {
        match self {
            DeliveryRx::Fanout(rx) => match rx.try_recv() {
                Ok(d) => TryRxOutcome::Got(d),
                Err(broadcast::error::TryRecvError::Lagged(n)) => TryRxOutcome::Lagged(n),
                Err(broadcast::error::TryRecvError::Empty) => TryRxOutcome::Empty,
                Err(broadcast::error::TryRecvError::Closed) => TryRxOutcome::Closed,
            },
            DeliveryRx::Queue(rx) => match rx.try_recv() {
                Ok(Some(d)) => TryRxOutcome::Got(d),
                Ok(None) => TryRxOutcome::Empty,
                Err(_) => TryRxOutcome::Closed,
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
