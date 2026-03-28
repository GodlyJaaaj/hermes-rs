use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use hermes_core::Subject;
use hermes_proto::{DurableServerMessage, EventEnvelope};
use hermes_store::{MessageStore, StoreError};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::subscription::{
    QueueGroupMember, SubjectSubscribers, SubscriptionId, SubscriptionReceiver,
};

// ---------------------------------------------------------------------------
// Durable consumer
// ---------------------------------------------------------------------------

/// Tracks a durable consumer's channel and config.
pub struct DurableConsumer {
    /// Unique connection ID to prevent stale unsubscribes from removing a newer consumer.
    pub connection_id: u64,
    pub consumer_name: String,
    pub subject_bytes: Vec<u8>,
    pub subject_pattern: Subject,
    pub sender: mpsc::Sender<DurableServerMessage>,
    pub ack_timeout_secs: u32,
    pub max_in_flight: u32,
}

// ---------------------------------------------------------------------------
// Wildcard entry
// ---------------------------------------------------------------------------

/// A wildcard subscription: parsed pattern + pre-partitioned subscribers.
struct WildcardEntry {
    subscribers: SubjectSubscribers,
}

// ---------------------------------------------------------------------------
// BrokerEngine
// ---------------------------------------------------------------------------

pub struct BrokerEngine {
    /// Exact subscriptions: Subject -> pre-partitioned subscribers (O(1) lookup).
    exact_subscriptions: DashMap<Subject, SubjectSubscribers>,
    /// Wildcard subscriptions keyed by their pattern Subject.
    wildcard_subscriptions: DashMap<Subject, WildcardEntry>,
    channel_capacity: usize,
    /// Optional store for durable mode. None = fire-and-forget only.
    store: Option<Arc<dyn MessageStore>>,
    /// Durable consumers (consumer_name -> DurableConsumer).
    durable_consumers: DashMap<String, DurableConsumer>,
    /// Global round-robin counter for queue group dispatch.
    rr_counter: AtomicU64,
}

impl BrokerEngine {
    /// Create a fire-and-forget only engine.
    pub fn new(channel_capacity: usize) -> Self {
        Self {
            exact_subscriptions: DashMap::new(),
            wildcard_subscriptions: DashMap::new(),
            channel_capacity,
            store: None,
            durable_consumers: DashMap::new(),
            rr_counter: AtomicU64::new(0),
        }
    }

    /// Create an engine with durable support.
    pub fn with_store(channel_capacity: usize, store: Arc<dyn MessageStore>) -> Self {
        Self {
            exact_subscriptions: DashMap::new(),
            wildcard_subscriptions: DashMap::new(),
            channel_capacity,
            store: Some(store),
            durable_consumers: DashMap::new(),
            rr_counter: AtomicU64::new(0),
        }
    }

    pub fn store(&self) -> Option<&Arc<dyn MessageStore>> {
        self.store.as_ref()
    }

    // -----------------------------------------------------------------------
    // Subscribe / Unsubscribe
    // -----------------------------------------------------------------------

    /// Register a new fire-and-forget subscription. Returns (id, receiver).
    ///
    /// - `queue_groups` empty → fanout via `broadcast` (zero-copy fan-out).
    /// - `queue_groups` non-empty → round-robin via `mpsc` per group member.
    pub fn subscribe(
        &self,
        subject: Subject,
        queue_groups: Vec<String>,
    ) -> (SubscriptionId, SubscriptionReceiver) {
        let id = Uuid::now_v7();

        let receiver = if subject.is_pattern() {
            self.subscribe_wildcard(id, subject.clone(), &queue_groups)
        } else {
            self.subscribe_exact(id, subject.clone(), &queue_groups)
        };

        debug!(subject = %subject, %id, "new subscription");
        (id, receiver)
    }

    fn subscribe_exact(
        &self,
        id: SubscriptionId,
        subject: Subject,
        queue_groups: &[String],
    ) -> SubscriptionReceiver {
        let mut entry = self
            .exact_subscriptions
            .entry(subject)
            .or_insert_with(|| SubjectSubscribers::new(self.channel_capacity));

        if queue_groups.is_empty() {
            SubscriptionReceiver::Fanout(entry.subscribe_fanout())
        } else {
            let (tx, rx) = mpsc::channel(self.channel_capacity);
            let member = QueueGroupMember { id, sender: tx };
            entry.add_to_groups(member, queue_groups);
            SubscriptionReceiver::QueueGroup(rx)
        }
    }

    fn subscribe_wildcard(
        &self,
        id: SubscriptionId,
        subject: Subject,
        queue_groups: &[String],
    ) -> SubscriptionReceiver {
        let mut entry = self
            .wildcard_subscriptions
            .entry(subject)
            .or_insert_with(|| WildcardEntry {
                subscribers: SubjectSubscribers::new(self.channel_capacity),
            });

        if queue_groups.is_empty() {
            SubscriptionReceiver::Fanout(entry.subscribers.subscribe_fanout())
        } else {
            let (tx, rx) = mpsc::channel(self.channel_capacity);
            let member = QueueGroupMember { id, sender: tx };
            entry.subscribers.add_to_groups(member, queue_groups);
            SubscriptionReceiver::QueueGroup(rx)
        }
    }

    /// Unsubscribe by id (queue group members only — fanout subscribers
    /// are automatically removed when their broadcast receiver is dropped).
    pub fn unsubscribe(&self, subject: &Subject, id: SubscriptionId) {
        if let Some(mut subs) = self.exact_subscriptions.get_mut(subject) {
            subs.remove_from_groups(id);
            if subs.is_empty() {
                drop(subs);
                self.exact_subscriptions.remove(subject);
            }
            debug!(subject = %subject, %id, "unsubscribed");
            return;
        }

        if let Some(mut we) = self.wildcard_subscriptions.get_mut(subject) {
            we.subscribers.remove_from_groups(id);
            if we.subscribers.is_empty() {
                drop(we);
                self.wildcard_subscriptions.remove(subject);
            }
        }
        debug!(subject = %subject, %id, "unsubscribed");
    }

    // -----------------------------------------------------------------------
    // Publish (fire-and-forget)
    // -----------------------------------------------------------------------

    /// Publish to fire-and-forget subscribers.
    /// Returns the number of subscribers that received the message.
    pub fn publish(&self, envelope: &EventEnvelope) -> usize {
        let subject = match Subject::from_bytes(&envelope.subject) {
            Ok(s) => s,
            Err(_) => return 0,
        };
        // Wrap once in Arc — all fanout + queue-group members share this.
        let arc_env = Arc::new(envelope.clone());

        let mut delivered: usize = 0;

        // 1) Exact match (O(1)).
        delivered += self.publish_exact(&subject, &arc_env);

        // 2) Wildcard match (iterate patterns).
        delivered += self.publish_wildcard(&subject, &arc_env);

        debug!(subject = %subject, id = %envelope.id, delivered, "publish completed");
        delivered
    }

    fn publish_exact(&self, subject: &Subject, arc_env: &Arc<EventEnvelope>) -> usize {
        let Some(mut subs) = self.exact_subscriptions.get_mut(subject) else {
            trace!(subject = %subject, "no exact subscribers");
            return 0;
        };
        self.deliver_to_subscribers(&mut subs, arc_env, subject)
    }

    fn publish_wildcard(&self, subject: &Subject, arc_env: &Arc<EventEnvelope>) -> usize {
        if self.wildcard_subscriptions.is_empty() {
            return 0;
        }

        let mut total = 0;
        for mut entry in self.wildcard_subscriptions.iter_mut() {
            if entry.key().matches(subject) {
                trace!(subject = %subject, pattern = %entry.key(), "wildcard match");
                let we = entry.value_mut();
                total += self.deliver_to_subscribers(&mut we.subscribers, arc_env, subject);
            }
        }
        total
    }

    /// Deliver an envelope to pre-partitioned subscribers.
    fn deliver_to_subscribers(
        &self,
        subs: &mut SubjectSubscribers,
        arc_env: &Arc<EventEnvelope>,
        subject: &Subject,
    ) -> usize {
        let fanout_count = self.deliver_fanout(&subs.fanout, arc_env);
        let group_count = self.deliver_groups(&mut subs.groups, arc_env, subject);
        fanout_count + group_count
    }

    /// Broadcast to all fanout subscribers via the broadcast channel.
    /// Returns the number of receivers that received the message.
    fn deliver_fanout(
        &self,
        fanout: &tokio::sync::broadcast::Sender<Arc<EventEnvelope>>,
        arc_env: &Arc<EventEnvelope>,
    ) -> usize {
        if fanout.receiver_count() == 0 {
            return 0;
        }
        let count = fanout.send(Arc::clone(arc_env)).unwrap_or(0);
        trace!(receivers = count, "fanout delivered");
        count
    }

    /// Round-robin dispatch to each queue group. Removes dead members in-place.
    fn deliver_groups(
        &self,
        groups: &mut std::collections::HashMap<String, Vec<QueueGroupMember>>,
        arc_env: &Arc<EventEnvelope>,
        subject: &Subject,
    ) -> usize {
        let mut delivered: usize = 0;
        groups.retain(|_group, members| {
            if members.is_empty() {
                return false;
            }
            delivered += self.deliver_one_group(members, arc_env, subject);
            !members.is_empty()
        });
        delivered
    }

    /// Pick one member via round-robin. If the picked member is dead, try the
    /// next one. Returns 0 or 1.
    fn deliver_one_group(
        &self,
        members: &mut Vec<QueueGroupMember>,
        arc_env: &Arc<EventEnvelope>,
        subject: &Subject,
    ) -> usize {
        let len = members.len();
        let start = self.rr_counter.fetch_add(1, Ordering::Relaxed) as usize;

        for i in 0..len {
            if members.is_empty() {
                return 0;
            }
            let idx = (start + i) % members.len();

            match members[idx].sender.try_send(Arc::clone(arc_env)) {
                Ok(()) => {
                    trace!(subject = %subject, member = %members[idx].id, "queue-group delivered");
                    return 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        subject = %subject,
                        id = %members[idx].id,
                        "queue group member full, trying next"
                    );
                    continue;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    members.swap_remove(idx);
                    continue;
                }
            }
        }
        0
    }

    // -----------------------------------------------------------------------
    // Durable publish
    // -----------------------------------------------------------------------

    /// Persist a message and dispatch to fire-and-forget + durable subscribers.
    pub fn publish_durable(&self, envelope: &EventEnvelope) -> Result<usize, StoreError> {
        let store = self.store.as_ref().ok_or(StoreError::NotConfigured)?;

        // Persist BEFORE dispatch.
        store.persist(envelope)?;

        // Fire-and-forget subscribers.
        let delivered = self.publish(envelope);

        // Durable consumers.
        self.dispatch_to_durable_consumers(store, envelope);

        debug!(id = %envelope.id, delivered, "durable publish completed");
        Ok(delivered)
    }

    fn dispatch_to_durable_consumers(
        &self,
        store: &Arc<dyn MessageStore>,
        envelope: &EventEnvelope,
    ) {
        let subject = Subject::from_bytes(&envelope.subject).ok();

        for entry in self.durable_consumers.iter() {
            let consumer = entry.value();

            if !self.consumer_matches(consumer, &envelope.subject, subject.as_ref()) {
                continue;
            }

            let now_ms = now_ms();
            let deadline = now_ms + u64::from(consumer.ack_timeout_secs) * 1000;

            trace!(consumer = %consumer.consumer_name, id = %envelope.id, "dispatching to durable consumer");

            if let Err(e) = store.mark_delivered(&envelope.id, &consumer.consumer_name, deadline) {
                warn!(
                    consumer = consumer.consumer_name,
                    id = envelope.id,
                    "failed to mark delivered: {e}"
                );
                continue;
            }

            let msg = DurableServerMessage {
                msg: Some(hermes_proto::durable_server_message::Msg::Envelope(
                    envelope.clone(),
                )),
            };

            if consumer.sender.try_send(msg).is_err() {
                warn!(
                    consumer = consumer.consumer_name,
                    "durable consumer channel full or closed"
                );
            }
        }
    }

    fn consumer_matches(
        &self,
        consumer: &DurableConsumer,
        subject_bytes: &[u8],
        subject: Option<&Subject>,
    ) -> bool {
        if consumer.subject_bytes == subject_bytes {
            return true;
        }
        if let Some(subj) = subject {
            return consumer.subject_pattern.matches(subj);
        }
        false
    }

    // -----------------------------------------------------------------------
    // Durable subscribe
    // -----------------------------------------------------------------------

    /// Register a durable consumer. Returns `(connection_id, receiver)`.
    /// The `connection_id` must be passed to `unsubscribe_durable` to prevent
    /// a stale disconnect from removing a newer consumer with the same name.
    pub fn subscribe_durable(
        &self,
        consumer_name: String,
        subject_bytes: Vec<u8>,
        queue_groups: Vec<String>,
        max_in_flight: u32,
        ack_timeout_secs: u32,
    ) -> Result<(u64, mpsc::Receiver<DurableServerMessage>), StoreError> {
        let store = self.store.as_ref().ok_or(StoreError::NotConfigured)?;

        store.register_consumer(&consumer_name, &subject_bytes, &queue_groups)?;

        let connection_id = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(max_in_flight as usize);

        let subject_pattern =
            Subject::from_bytes(&subject_bytes).unwrap_or_else(|_| Subject::new());

        let consumer = DurableConsumer {
            connection_id,
            consumer_name: consumer_name.clone(),
            subject_bytes: subject_bytes.clone(),
            subject_pattern,
            sender: tx,
            ack_timeout_secs,
            max_in_flight,
        };

        self.durable_consumers
            .insert(consumer_name.clone(), consumer);

        self.catch_up_durable(store, &consumer_name, max_in_flight, ack_timeout_secs);

        debug!(
            consumer_name,
            connection_id, "durable subscription registered"
        );
        Ok((connection_id, rx))
    }

    fn catch_up_durable(
        &self,
        store: &Arc<dyn MessageStore>,
        consumer_name: &str,
        max_in_flight: u32,
        ack_timeout_secs: u32,
    ) {
        let pending = match store.fetch_pending(consumer_name, max_in_flight) {
            Ok(p) => p,
            Err(e) => {
                warn!(consumer_name, "catch-up: fetch_pending failed: {e}");
                return;
            }
        };

        let pending_count = pending.len();

        for stored in pending {
            let msg_id = stored.envelope.id.clone();
            let attempt = stored.attempt;
            let deadline = now_ms() + u64::from(ack_timeout_secs) * 1000;
            let _ = store.mark_delivered(&msg_id, consumer_name, deadline);

            let msg = if stored.attempt > 1 {
                DurableServerMessage {
                    msg: Some(hermes_proto::durable_server_message::Msg::Redelivery(
                        hermes_proto::Redelivery {
                            envelope: Some(stored.envelope),
                            attempt: stored.attempt,
                        },
                    )),
                }
            } else {
                DurableServerMessage {
                    msg: Some(hermes_proto::durable_server_message::Msg::Envelope(
                        stored.envelope,
                    )),
                }
            };

            if let Some(consumer) = self.durable_consumers.get(consumer_name) {
                let _ = consumer.sender.try_send(msg);
            }
            trace!(consumer_name, id = %msg_id, attempt, "catch-up: delivering pending message");
        }

        if pending_count > 0 {
            debug!(consumer_name, pending = pending_count, "catch-up completed");
        }
    }

    /// Remove a durable consumer (on disconnect).
    /// Only removes if the `connection_id` matches the current entry, preventing
    /// a stale disconnect from removing a newer consumer that reconnected.
    pub fn unsubscribe_durable(&self, consumer_name: &str, connection_id: u64) {
        self.durable_consumers
            .remove_if(consumer_name, |_k, v| v.connection_id == connection_id);
        debug!(
            consumer_name,
            connection_id, "durable consumer disconnected"
        );
    }

    // -----------------------------------------------------------------------
    // Ack / Nack
    // -----------------------------------------------------------------------

    /// Acknowledge a durable message.
    pub fn ack_message(&self, message_id: &str, consumer_name: &str) -> Result<(), StoreError> {
        let store = self.store.as_ref().ok_or(StoreError::NotConfigured)?;
        store.ack(message_id, consumer_name)
    }

    /// Negative acknowledge a durable message.
    pub fn nack_message(
        &self,
        message_id: &str,
        consumer_name: &str,
        requeue: bool,
    ) -> Result<(), StoreError> {
        let store = self.store.as_ref().ok_or(StoreError::NotConfigured)?;
        store.nack(message_id, consumer_name, requeue)
    }

    /// Access durable consumers (for redelivery loop).
    pub fn durable_consumers(&self) -> &DashMap<String, DurableConsumer> {
        &self.durable_consumers
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::SubscriptionReceiver;

    fn make_envelope(subject: &Subject) -> EventEnvelope {
        EventEnvelope {
            id: "1".into(),
            subject: subject.to_bytes(),
            payload: vec![1, 2, 3],
            headers: Default::default(),
            timestamp_nanos: 0,
        }
    }

    /// Helper: blocking try_recv from a SubscriptionReceiver.
    fn try_recv(rx: &mut SubscriptionReceiver) -> Option<Arc<EventEnvelope>> {
        match rx {
            SubscriptionReceiver::Fanout(r) => r.try_recv().ok(),
            SubscriptionReceiver::QueueGroup(r) => r.try_recv().ok(),
        }
    }

    fn drain_count(rx: &mut SubscriptionReceiver) -> usize {
        let mut count = 0;
        while try_recv(rx).is_some() {
            count += 1;
        }
        count
    }

    #[tokio::test]
    async fn test_fanout() {
        let engine = BrokerEngine::new(16);
        let subject = Subject::new().str("test").str("Subject");

        let (_id1, mut rx1) = engine.subscribe(subject.clone(), vec![]);
        let (_id2, mut rx2) = engine.subscribe(subject.clone(), vec![]);

        let envelope = make_envelope(&subject);
        let delivered = engine.publish(&envelope);
        assert_eq!(delivered, 2);

        assert!(try_recv(&mut rx1).is_some());
        assert!(try_recv(&mut rx2).is_some());
    }

    #[tokio::test]
    async fn test_queue_group() {
        let engine = BrokerEngine::new(16);
        let subject = Subject::new().str("test").str("QG");

        let (_id1, mut rx1) = engine.subscribe(subject.clone(), vec!["workers".into()]);
        let (_id2, mut rx2) = engine.subscribe(subject.clone(), vec!["workers".into()]);

        // Single message: exactly one receives it
        let envelope = make_envelope(&subject);
        let delivered = engine.publish(&envelope);
        assert_eq!(delivered, 1);

        let got1 = try_recv(&mut rx1).is_some();
        let got2 = try_recv(&mut rx2).is_some();
        assert!(got1 ^ got2, "exactly one should receive the message");

        // Two messages: round-robin distributes across both subscribers
        let e1 = make_envelope(&subject);
        let e2 = make_envelope(&subject);
        engine.publish(&e1);
        engine.publish(&e2);

        let count1 = drain_count(&mut rx1);
        let count2 = drain_count(&mut rx2);
        assert_eq!(count1 + count2, 2, "both messages should be delivered");
        assert!(
            count1 >= 1 && count2 >= 1,
            "round-robin should distribute: got {count1} and {count2}"
        );
    }

    #[tokio::test]
    async fn test_multiple_queue_groups() {
        let engine = BrokerEngine::new(16);
        let subject = Subject::new().str("test").str("MultiQG");

        // Sub1 is in both "workers" and "loggers"
        let (_id1, mut rx1) =
            engine.subscribe(subject.clone(), vec!["workers".into(), "loggers".into()]);
        // Sub2 is only in "workers"
        let (_id2, mut rx2) = engine.subscribe(subject.clone(), vec!["workers".into()]);
        // Sub3 is only in "loggers"
        let (_id3, mut rx3) = engine.subscribe(subject.clone(), vec!["loggers".into()]);

        let envelope = make_envelope(&subject);
        let delivered = engine.publish(&envelope);
        // 1 for "workers" group + 1 for "loggers" group = 2
        assert_eq!(delivered, 2);

        let got1 = drain_count(&mut rx1);
        let got2 = drain_count(&mut rx2);
        let got3 = drain_count(&mut rx3);
        assert_eq!(got1 + got2 + got3, 2, "two groups = two deliveries");
    }

    #[tokio::test]
    async fn test_fanout_and_queue_group_coexist() {
        let engine = BrokerEngine::new(16);
        let subject = Subject::new().str("test").str("Mixed");

        // Fanout observer
        let (_id1, mut rx_fanout) = engine.subscribe(subject.clone(), vec![]);
        // Queue group workers
        let (_id2, mut rx_w1) = engine.subscribe(subject.clone(), vec!["workers".into()]);
        let (_id3, mut rx_w2) = engine.subscribe(subject.clone(), vec!["workers".into()]);

        let envelope = make_envelope(&subject);
        let delivered = engine.publish(&envelope);
        // 1 fanout + 1 from workers group = 2
        assert_eq!(delivered, 2);

        assert!(try_recv(&mut rx_fanout).is_some());
        let w1 = try_recv(&mut rx_w1).is_some();
        let w2 = try_recv(&mut rx_w2).is_some();
        assert!(w1 ^ w2, "exactly one worker should get it");
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let engine = BrokerEngine::new(16);
        let subject = Subject::new().str("test").str("Unsub");

        let (_id1, rx1) = engine.subscribe(subject.clone(), vec![]);
        let (_id2, mut rx2) = engine.subscribe(subject.clone(), vec![]);

        // Dropping the fanout receiver unsubscribes automatically (broadcast).
        drop(rx1);

        let envelope = make_envelope(&subject);
        let delivered = engine.publish(&envelope);
        assert_eq!(delivered, 1);
        assert!(try_recv(&mut rx2).is_some());
    }

    #[tokio::test]
    async fn test_no_subscribers() {
        let engine = BrokerEngine::new(16);
        let subject = Subject::new().str("test").str("NoOne");
        let envelope = make_envelope(&subject);
        assert_eq!(engine.publish(&envelope), 0);
    }

    #[tokio::test]
    async fn test_wildcard_subscription() {
        let engine = BrokerEngine::new(16);

        // Subscribe with wildcard: job.*.logs
        let pattern = Subject::new().str("job").any().str("logs");
        let (_id, mut rx) = engine.subscribe(pattern, vec![]);

        // Publish job.42.logs
        let subject = Subject::new().str("job").int(42).str("logs");
        let envelope = make_envelope(&subject);
        let delivered = engine.publish(&envelope);
        assert_eq!(delivered, 1);
        assert!(try_recv(&mut rx).is_some());

        // Publish job.abc.logs — should also match
        let subject2 = Subject::new().str("job").str("abc").str("logs");
        let envelope2 = make_envelope(&subject2);
        let delivered2 = engine.publish(&envelope2);
        assert_eq!(delivered2, 1);
        assert!(try_recv(&mut rx).is_some());

        // Publish other.42.logs — should NOT match
        let subject3 = Subject::new().str("other").int(42).str("logs");
        let envelope3 = make_envelope(&subject3);
        let delivered3 = engine.publish(&envelope3);
        assert_eq!(delivered3, 0);
    }

    #[tokio::test]
    async fn test_multi_wildcard_subscription() {
        let engine = BrokerEngine::new(16);

        // Subscribe with multi-wildcard: job.>
        let pattern = Subject::new().str("job").rest();
        let (_id, mut rx) = engine.subscribe(pattern, vec![]);

        // Publish job — matches (> matches 0 trailing)
        let envelope1 = make_envelope(&Subject::new().str("job"));
        assert_eq!(engine.publish(&envelope1), 1);
        assert!(try_recv(&mut rx).is_some());

        // Publish job.42.logs — matches
        let envelope2 = make_envelope(&Subject::new().str("job").int(42).str("logs"));
        assert_eq!(engine.publish(&envelope2), 1);
        assert!(try_recv(&mut rx).is_some());

        // Publish other — no match
        let envelope3 = make_envelope(&Subject::new().str("other"));
        assert_eq!(engine.publish(&envelope3), 0);
    }

    #[tokio::test]
    async fn test_dead_subscriber_cleanup() {
        let engine = BrokerEngine::new(16);
        let subject = Subject::new().str("test").str("Dead");

        // Two fanout subscribers
        let (_id1, rx1) = engine.subscribe(subject.clone(), vec![]);
        let (_id2, mut rx2) = engine.subscribe(subject.clone(), vec![]);

        // Drop rx1 — broadcast automatically stops delivering to it.
        drop(rx1);

        let envelope = make_envelope(&subject);
        let delivered = engine.publish(&envelope);
        assert_eq!(delivered, 1);
        assert!(try_recv(&mut rx2).is_some());

        // broadcast::Sender::receiver_count() should reflect the drop.
        let subs = engine.exact_subscriptions.get(&subject).unwrap();
        assert_eq!(subs.fanout.receiver_count(), 1);
    }
}
