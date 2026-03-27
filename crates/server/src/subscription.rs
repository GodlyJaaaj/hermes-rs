use std::collections::HashMap;
use std::sync::Arc;

use hermes_proto::EventEnvelope;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

pub type SubscriptionId = Uuid;

/// Receiver returned by [`super::broker::BrokerEngine::subscribe`].
pub enum SubscriptionReceiver {
    /// Fanout: receives all messages via broadcast (zero-copy fan-out).
    Fanout(broadcast::Receiver<Arc<EventEnvelope>>),
    /// Queue group: receives messages via round-robin mpsc.
    QueueGroup(mpsc::Receiver<Arc<EventEnvelope>>),
}

/// A queue-group member: identity + mpsc channel.
#[derive(Clone)]
pub struct QueueGroupMember {
    pub id: SubscriptionId,
    pub sender: mpsc::Sender<Arc<EventEnvelope>>,
}

/// Pre-partitioned subscribers for a single subject.
///
/// **Fanout** uses a `broadcast` channel: one `send()` delivers to all
/// receivers without per-subscriber cloning — the `Arc` is cloned internally.
///
/// **Queue groups** keep per-member `mpsc` channels for round-robin selection.
pub struct SubjectSubscribers {
    /// Broadcast channel for fanout delivery.
    pub fanout: broadcast::Sender<Arc<EventEnvelope>>,
    /// Queue groups: one member per group receives each message.
    pub groups: HashMap<String, Vec<QueueGroupMember>>,
}

impl SubjectSubscribers {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            fanout: tx,
            groups: HashMap::new(),
        }
    }

    /// Create a new fanout receiver from the broadcast channel.
    pub fn subscribe_fanout(&self) -> broadcast::Receiver<Arc<EventEnvelope>> {
        self.fanout.subscribe()
    }

    /// Add a member to one or more queue groups.
    pub fn add_to_groups(&mut self, member: QueueGroupMember, queue_groups: &[String]) {
        for group in queue_groups {
            self.groups
                .entry(group.clone())
                .or_default()
                .push(member.clone());
        }
    }

    /// Remove a member by id from all queue groups.
    pub fn remove_from_groups(&mut self, id: SubscriptionId) {
        self.groups.retain(|_, members| {
            members.retain(|m| m.id != id);
            !members.is_empty()
        });
    }

    /// `true` if no fanout receivers and no group members remain.
    pub fn is_empty(&self) -> bool {
        self.fanout.receiver_count() == 0 && self.groups.is_empty()
    }
}
