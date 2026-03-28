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
    pub queue_groups: Vec<String>,
    pub sender: mpsc::Sender<DurableServerMessage>,
    pub ack_timeout_secs: u32,
    pub max_in_flight: u32,
}

impl DurableConsumer {
    /// Check if this consumer matches the given subject (exact or pattern).
    fn matches(&self, subject_bytes: &[u8], subject: Option<&Subject>) -> bool {
        self.subject_bytes == subject_bytes
            || subject.is_some_and(|s| self.subject_pattern.matches(s))
    }
}

// ---------------------------------------------------------------------------
// BrokerEngine
// ---------------------------------------------------------------------------

pub struct BrokerEngine {
    /// Exact subscriptions: Subject -> pre-partitioned subscribers (O(1) lookup).
    exact_subscriptions: DashMap<Subject, SubjectSubscribers>,
    /// Wildcard subscriptions keyed by their pattern Subject.
    wildcard_subscriptions: DashMap<Subject, SubjectSubscribers>,
    channel_capacity: usize,
    /// Optional store for durable mode. None = fire-and-forget only.
    store: Option<Arc<dyn MessageStore>>,
    /// Durable consumers (consumer_name -> DurableConsumer).
    durable_consumers: DashMap<String, DurableConsumer>,
    /// Global round-robin counter for queue group dispatch.
    rr_counter: AtomicU64,
}

impl BrokerEngine {
    /// Create an engine. Pass `None` for fire-and-forget only, or `Some(store)` for durable.
    pub fn new(channel_capacity: usize, store: Option<Arc<dyn MessageStore>>) -> Self {
        Self {
            exact_subscriptions: DashMap::new(),
            wildcard_subscriptions: DashMap::new(),
            channel_capacity,
            store,
            durable_consumers: DashMap::new(),
            rr_counter: AtomicU64::new(0),
        }
    }

    pub fn store(&self) -> Option<&Arc<dyn MessageStore>> {
        self.store.as_ref()
    }

    /// Return the next round-robin index for a group of `count` members.
    fn next_round_robin(&self, count: usize) -> usize {
        self.rr_counter.fetch_add(1, Ordering::Relaxed) as usize % count
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

        let map = if subject.is_pattern() {
            &self.wildcard_subscriptions
        } else {
            &self.exact_subscriptions
        };

        let mut entry = map
            .entry(subject.clone())
            .or_insert_with(|| SubjectSubscribers::new(self.channel_capacity));

        let receiver = if queue_groups.is_empty() {
            SubscriptionReceiver::Fanout(entry.subscribe_fanout())
        } else {
            let (tx, rx) = mpsc::channel(self.channel_capacity);
            entry.add_to_groups(QueueGroupMember { id, sender: tx }, &queue_groups);
            SubscriptionReceiver::QueueGroup(rx)
        };

        debug!(subject = %subject, %id, "new subscription");
        (id, receiver)
    }

    /// Unsubscribe by id (queue group members only — fanout subscribers
    /// are automatically removed when their broadcast receiver is dropped).
    pub fn unsubscribe(&self, subject: &Subject, id: SubscriptionId) {
        // Try exact first, then wildcard.
        for map in [&self.exact_subscriptions, &self.wildcard_subscriptions] {
            if let Some(mut subs) = map.get_mut(subject) {
                subs.remove_from_groups(id);
                if subs.is_empty() {
                    drop(subs);
                    map.remove(subject);
                }
                debug!(subject = %subject, %id, "unsubscribed");
                return;
            }
        }
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
        let arc_env = Arc::new(envelope.clone());
        let mut delivered: usize = 0;

        // 1) Exact match (O(1)).
        if let Some(mut subs) = self.exact_subscriptions.get_mut(&subject) {
            delivered += self.deliver(&mut subs, &arc_env, &subject);
        }

        // 2) Wildcard match (iterate patterns).
        for mut entry in self.wildcard_subscriptions.iter_mut() {
            if entry.key().matches(&subject) {
                trace!(subject = %subject, pattern = %entry.key(), "wildcard match");
                delivered += self.deliver(entry.value_mut(), &arc_env, &subject);
            }
        }

        debug!(subject = %subject, id = %envelope.id, delivered, "publish completed");
        delivered
    }

    /// Deliver an envelope to a subject's subscribers (fanout broadcast + queue groups).
    fn deliver(
        &self,
        subs: &mut SubjectSubscribers,
        arc_env: &Arc<EventEnvelope>,
        subject: &Subject,
    ) -> usize {
        // Fanout broadcast.
        let mut count = if subs.fanout.receiver_count() > 0 {
            subs.fanout.send(Arc::clone(arc_env)).unwrap_or(0)
        } else {
            0
        };

        // Queue groups: round-robin one member per group, prune dead members.
        subs.groups.retain(|_group, members| {
            if !members.is_empty() {
                count += self.deliver_to_group(members, arc_env, subject);
            }
            !members.is_empty()
        });

        count
    }

    /// Pick one member via round-robin. If the picked member is dead, try the
    /// next one. Returns 0 or 1.
    fn deliver_to_group(
        &self,
        members: &mut Vec<QueueGroupMember>,
        arc_env: &Arc<EventEnvelope>,
        subject: &Subject,
    ) -> usize {
        let start = self.next_round_robin(members.len());
        let len = members.len();

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
                    warn!(subject = %subject, id = %members[idx].id, "queue group member full, trying next");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    members.swap_remove(idx);
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
        store.persist(envelope)?;

        let delivered = self.publish(envelope);
        self.dispatch_to_durable_consumers(store, envelope);

        debug!(id = %envelope.id, delivered, "durable publish completed");
        Ok(delivered)
    }

    fn dispatch_to_durable_consumers(
        &self,
        store: &Arc<dyn MessageStore>,
        envelope: &EventEnvelope,
    ) {
        use std::collections::HashMap;

        let subject = Subject::from_bytes(&envelope.subject).ok();

        // Partition matching consumers into fanout (no groups) and per-group buckets.
        let mut targets: Vec<(String, u32, mpsc::Sender<DurableServerMessage>)> = Vec::new();
        let mut groups: HashMap<String, Vec<(String, u32, mpsc::Sender<DurableServerMessage>)>> =
            HashMap::new();

        for entry in self.durable_consumers.iter() {
            let c = entry.value();
            if !c.matches(&envelope.subject, subject.as_ref()) {
                continue;
            }
            let tuple = (
                c.consumer_name.clone(),
                c.ack_timeout_secs,
                c.sender.clone(),
            );
            if c.queue_groups.is_empty() {
                targets.push(tuple);
            } else {
                for group in &c.queue_groups {
                    groups.entry(group.clone()).or_default().push(tuple.clone());
                }
            }
        }

        // Select one consumer per queue group via round-robin.
        for members in groups.into_values() {
            let idx = self.next_round_robin(members.len());
            if let Some(picked) = members.into_iter().nth(idx) {
                targets.push(picked);
            }
        }

        // Dispatch to all selected consumers.
        for (name, ack_timeout_secs, sender) in &targets {
            let deadline = now_ms() + u64::from(*ack_timeout_secs) * 1000;

            if let Err(e) = store.mark_delivered(&envelope.id, name, deadline) {
                warn!(consumer = %name, id = %envelope.id, "failed to mark delivered: {e}");
                continue;
            }

            let msg = DurableServerMessage {
                msg: Some(hermes_proto::durable_server_message::Msg::Envelope(
                    envelope.clone(),
                )),
            };

            if sender.try_send(msg).is_err() {
                warn!(consumer = %name, "durable consumer channel full or closed");
            }
        }
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

        self.durable_consumers.insert(
            consumer_name.clone(),
            DurableConsumer {
                connection_id,
                consumer_name: consumer_name.clone(),
                subject_bytes: subject_bytes.clone(),
                subject_pattern,
                queue_groups,
                sender: tx,
                ack_timeout_secs,
                max_in_flight,
            },
        );

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

        if pending.is_empty() {
            return;
        }

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

        debug!(consumer_name, pending = pending_count, "catch-up completed");
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

    fn engine() -> BrokerEngine {
        BrokerEngine::new(16, None)
    }

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
        let engine = engine();
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
        let engine = engine();
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
        let engine = engine();
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
        let engine = engine();
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
        let engine = engine();
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
        let engine = engine();
        let subject = Subject::new().str("test").str("NoOne");
        let envelope = make_envelope(&subject);
        assert_eq!(engine.publish(&envelope), 0);
    }

    #[tokio::test]
    async fn test_wildcard_subscription() {
        let engine = engine();

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
        let engine = engine();

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
        let engine = engine();
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

    // -- Durable queue group tests --

    fn durable_engine() -> BrokerEngine {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let store = Arc::new(hermes_store::RedbMessageStore::open(tmp.path()).unwrap())
            as Arc<dyn MessageStore>;
        std::mem::forget(tmp); // keep tempfile alive for test duration
        BrokerEngine::new(16, Some(store))
    }

    fn make_durable_envelope(subject: &Subject, id: &str) -> EventEnvelope {
        EventEnvelope {
            id: id.into(),
            subject: subject.to_bytes(),
            payload: vec![1, 2, 3],
            headers: Default::default(),
            timestamp_nanos: 0,
        }
    }

    #[tokio::test]
    async fn test_durable_queue_group_round_robin() {
        let engine = durable_engine();
        let subject = Subject::new().str("durable").str("qg");
        let subject_bytes = subject.to_bytes();

        let (_cid1, mut rx1) = engine
            .subscribe_durable(
                "c1".into(),
                subject_bytes.clone(),
                vec!["workers".into()],
                10,
                30,
            )
            .unwrap();
        let (_cid2, mut rx2) = engine
            .subscribe_durable(
                "c2".into(),
                subject_bytes.clone(),
                vec!["workers".into()],
                10,
                30,
            )
            .unwrap();

        // Publish 2 messages — each consumer should get exactly 1 (round-robin).
        engine
            .publish_durable(&make_durable_envelope(&subject, "msg-1"))
            .unwrap();
        engine
            .publish_durable(&make_durable_envelope(&subject, "msg-2"))
            .unwrap();

        // Small yield to let channels propagate.
        tokio::task::yield_now().await;

        let got1 = std::iter::from_fn(|| rx1.try_recv().ok()).count();
        let got2 = std::iter::from_fn(|| rx2.try_recv().ok()).count();

        assert_eq!(got1 + got2, 2, "total should be 2");
        assert!(
            got1 >= 1 && got2 >= 1,
            "round-robin should distribute: c1={got1}, c2={got2}"
        );
    }

    #[tokio::test]
    async fn test_durable_no_queue_group_is_fanout() {
        let engine = durable_engine();
        let subject = Subject::new().str("durable").str("fanout");
        let subject_bytes = subject.to_bytes();

        let (_cid1, mut rx1) = engine
            .subscribe_durable("c1".into(), subject_bytes.clone(), vec![], 10, 30)
            .unwrap();
        let (_cid2, mut rx2) = engine
            .subscribe_durable("c2".into(), subject_bytes.clone(), vec![], 10, 30)
            .unwrap();

        engine
            .publish_durable(&make_durable_envelope(&subject, "msg-1"))
            .unwrap();

        tokio::task::yield_now().await;

        let got1 = std::iter::from_fn(|| rx1.try_recv().ok()).count();
        let got2 = std::iter::from_fn(|| rx2.try_recv().ok()).count();

        // Fanout: both consumers should get the message.
        assert_eq!(got1, 1, "c1 should receive 1");
        assert_eq!(got2, 1, "c2 should receive 1");
    }

    #[tokio::test]
    async fn test_durable_mixed_fanout_and_queue_group() {
        let engine = durable_engine();
        let subject = Subject::new().str("durable").str("mixed");
        let subject_bytes = subject.to_bytes();

        // Observer (no group) — always gets the message.
        let (_cid1, mut rx_observer) = engine
            .subscribe_durable("observer".into(), subject_bytes.clone(), vec![], 10, 30)
            .unwrap();

        // Two workers in same group — only one gets it.
        let (_cid2, mut rx_w1) = engine
            .subscribe_durable(
                "w1".into(),
                subject_bytes.clone(),
                vec!["workers".into()],
                10,
                30,
            )
            .unwrap();
        let (_cid3, mut rx_w2) = engine
            .subscribe_durable(
                "w2".into(),
                subject_bytes.clone(),
                vec!["workers".into()],
                10,
                30,
            )
            .unwrap();

        engine
            .publish_durable(&make_durable_envelope(&subject, "msg-1"))
            .unwrap();

        tokio::task::yield_now().await;

        let obs = std::iter::from_fn(|| rx_observer.try_recv().ok()).count();
        let w1 = std::iter::from_fn(|| rx_w1.try_recv().ok()).count();
        let w2 = std::iter::from_fn(|| rx_w2.try_recv().ok()).count();

        assert_eq!(obs, 1, "observer should always receive");
        assert_eq!(w1 + w2, 1, "exactly one worker should receive");
    }

    #[tokio::test]
    async fn test_durable_multiple_queue_groups() {
        let engine = durable_engine();
        let subject = Subject::new().str("durable").str("multi");
        let subject_bytes = subject.to_bytes();

        // c1 in "workers", c2 in "workers", c3 in "loggers"
        let (_cid1, mut rx1) = engine
            .subscribe_durable(
                "c1".into(),
                subject_bytes.clone(),
                vec!["workers".into()],
                10,
                30,
            )
            .unwrap();
        let (_cid2, mut rx2) = engine
            .subscribe_durable(
                "c2".into(),
                subject_bytes.clone(),
                vec!["workers".into()],
                10,
                30,
            )
            .unwrap();
        let (_cid3, mut rx3) = engine
            .subscribe_durable(
                "c3".into(),
                subject_bytes.clone(),
                vec!["loggers".into()],
                10,
                30,
            )
            .unwrap();

        engine
            .publish_durable(&make_durable_envelope(&subject, "msg-1"))
            .unwrap();

        tokio::task::yield_now().await;

        let got1 = std::iter::from_fn(|| rx1.try_recv().ok()).count();
        let got2 = std::iter::from_fn(|| rx2.try_recv().ok()).count();
        let got3 = std::iter::from_fn(|| rx3.try_recv().ok()).count();

        // "loggers" group has only c3 — must receive.
        assert_eq!(got3, 1, "c3 (loggers) should receive");
        // "workers" group: exactly one of c1/c2.
        assert_eq!(got1 + got2, 1, "exactly one worker should receive");
    }
}
