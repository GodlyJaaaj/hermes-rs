mod error;
mod redb_store;

pub use error::StoreError;
pub use redb_store::RedbMessageStore;

use hermes_proto::EventEnvelope;

/// A stored message with its delivery metadata.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub envelope: EventEnvelope,
    pub attempt: u32,
}

/// Delivery state for a message per consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryState {
    /// Persisted but not yet sent to the consumer.
    Pending,
    /// Sent to the consumer, waiting for ack.
    Delivered,
    /// Consumer acknowledged processing.
    Acked,
    /// Moved to dead-letter after max attempts.
    DeadLettered,
}

impl DeliveryState {
    fn as_u8(self) -> u8 {
        match self {
            Self::Pending => 0,
            Self::Delivered => 1,
            Self::Acked => 2,
            Self::DeadLettered => 3,
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Pending),
            1 => Some(Self::Delivered),
            2 => Some(Self::Acked),
            3 => Some(Self::DeadLettered),
            _ => None,
        }
    }
}

/// Trait abstracting message persistence for at-least-once delivery.
pub trait MessageStore: Send + Sync + 'static {
    /// Persist a message. Called before any dispatch.
    fn persist(&self, envelope: &EventEnvelope) -> Result<(), StoreError>;

    /// Register a durable consumer for a subject.
    fn register_consumer(
        &self,
        consumer_name: &str,
        subject: &str,
        queue_groups: &[String],
    ) -> Result<(), StoreError>;

    /// Fetch messages not yet delivered to this consumer, up to `limit`.
    fn fetch_pending(
        &self,
        consumer_name: &str,
        limit: u32,
    ) -> Result<Vec<StoredMessage>, StoreError>;

    /// Mark a message as delivered to a consumer with an ack deadline.
    fn mark_delivered(
        &self,
        message_id: &str,
        consumer_name: &str,
        ack_deadline_ms: u64,
    ) -> Result<(), StoreError>;

    /// Acknowledge a message. It won't be redelivered.
    fn ack(&self, message_id: &str, consumer_name: &str) -> Result<(), StoreError>;

    /// Negative acknowledge. If `requeue` is true, reset to pending for
    /// immediate redelivery. If false, move to dead-letter.
    fn nack(&self, message_id: &str, consumer_name: &str, requeue: bool) -> Result<(), StoreError>;

    /// Fetch messages delivered but not acked before `now_ms`.
    fn fetch_expired(
        &self,
        consumer_name: &str,
        now_ms: u64,
        limit: u32,
    ) -> Result<Vec<StoredMessage>, StoreError>;

    /// Delete acked messages older than `older_than_ms`.
    /// Returns the number of messages removed.
    fn gc_acked(&self, older_than_ms: u64) -> Result<u64, StoreError>;

    /// List all registered consumer names.
    fn list_consumers(&self) -> Result<Vec<String>, StoreError>;
}
