use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};

use crate::slot::{Delivery, Slot, SlotMap, SubHandle};
use crate::trie::{SlotId, TrieNode};

/// Commands sent to the router from gRPC tasks.
pub enum RouterCmd {
    /// Fire-and-forget publish. No reply — sequence is assigned internally.
    Publish {
        subject: Box<str>,
        payload: Bytes,
        reply_to: Option<Box<str>>,
    },
    /// Subscribe to a subject. Oneshot reply with the subscription handle.
    /// This is not on the hot path (happens once per subscription setup).
    Subscribe {
        subject: Box<str>,
        queue_group: Option<Box<str>>,
        reply: oneshot::Sender<SubHandle>,
    },
    /// Clean up a subscriber (stream closed / disconnected).
    Disconnect { sub_id: crate::slot::SubId },
}

/// Configuration for the router.
pub struct RouterConfig {
    /// Capacity of the broadcast channel for fanout slots.
    pub broadcast_capacity: usize,
    /// Capacity of the mpsc channel for queue-group members.
    pub queue_channel_capacity: usize,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            broadcast_capacity: 4096,
            queue_channel_capacity: 256,
        }
    }
}

/// The single-owner router task. Owns the trie and slot map.
pub struct Router {
    rx: mpsc::Receiver<RouterCmd>,
    trie: TrieNode,
    slots: SlotMap,
    sequence: u64,
    config: RouterConfig,
}

impl Router {
    /// Create a new router and its command sender.
    pub fn new(config: RouterConfig, channel_capacity: usize) -> (Self, mpsc::Sender<RouterCmd>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let router = Self {
            rx,
            trie: TrieNode::new(),
            slots: SlotMap::new(),
            sequence: 0,
            config,
        };
        (router, tx)
    }

    /// Run the router loop. This never returns until all senders are dropped.
    pub async fn run(mut self) {
        info!("router task started");

        // Scratch buffer reused across publishes — clear, don't realloc.
        let mut matched: Vec<SlotId> = Vec::with_capacity(64);

        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                RouterCmd::Publish {
                    subject,
                    payload,
                    reply_to,
                } => {
                    self.sequence += 1;
                    let seq = self.sequence;

                    matched.clear();
                    let tokens: Vec<&str> = subject.split('.').collect();
                    self.trie.lookup(&tokens, &mut matched);

                    if matched.is_empty() {
                        trace!(subject = %subject, seq, "publish has no matching slots");
                        continue;
                    }

                    // Deduplicate (a subject can match the same slot via multiple trie paths).
                    matched.sort_unstable_by_key(|s| s.0);
                    matched.dedup();

                    debug!(
                        subject = %subject,
                        seq,
                        matched_slots = matched.len(),
                        payload_bytes = payload.len(),
                        "publishing message"
                    );

                    let delivery = Delivery {
                        subject,
                        payload,
                        sequence: seq,
                        reply_to,
                    };

                    for &slot_id in &matched {
                        match self.slots.get_mut(&slot_id) {
                            Some(Slot::Broadcast { sender, .. }) => {
                                let _ = sender.send(delivery.clone());
                            }
                            Some(Slot::QueueGroup {
                                tx, member_ids, ..
                            }) => {
                                if member_ids.is_empty() {
                                    warn!(slot_id = slot_id.0, "queue group has no members");
                                } else {
                                    // Await until a member reads. The bounded channel
                                    // applies backpressure to publishers instead of
                                    // silently dropping — filling shouldn't happen in
                                    // practice, but we'd rather block than lose data.
                                    let _ = tx.send(delivery.clone()).await;
                                }
                            }
                            None => {
                                warn!(slot_id = slot_id.0, "matched slot not found in slot map");
                            }
                        }
                    }
                }

                RouterCmd::Subscribe {
                    subject,
                    queue_group,
                    reply,
                } => {
                    let tokens: Vec<&str> = subject.split('.').collect();

                    let (handle, new_slot_id) = match queue_group {
                        None => self
                            .slots
                            .subscribe_fanout(&subject, self.config.broadcast_capacity),
                        Some(ref group) => self.slots.subscribe_queue_group(
                            &subject,
                            group,
                            self.config.queue_channel_capacity,
                        ),
                    };

                    if let Some(slot_id) = new_slot_id {
                        self.trie.insert(&tokens, slot_id);
                    }

                    let sub_id = match &handle {
                        SubHandle::Fanout { sub_id, .. }
                        | SubHandle::QueueMember { sub_id, .. } => sub_id.0,
                    };

                    info!(
                        subject = %subject,
                        queue_group = queue_group.as_deref().unwrap_or("(none)"),
                        sub_id,
                        new_slot = new_slot_id.map(|s| s.0),
                        "subscription created"
                    );

                    let _ = reply.send(handle);
                }

                RouterCmd::Disconnect { sub_id } => {
                    let empty_slots = self.slots.remove_subscriber(sub_id);

                    info!(
                        sub_id = sub_id.0,
                        removed_slots = empty_slots.len(),
                        "subscriber disconnected"
                    );

                    for (slot_id, subject) in &empty_slots {
                        debug!(
                            slot_id = slot_id.0,
                            subject = %subject,
                            "removing empty slot from trie"
                        );
                        self.trie.remove(*slot_id);
                    }
                }
            }
        }

        info!("router task stopped (all senders dropped)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slot::SubHandle;

    async fn setup() -> mpsc::Sender<RouterCmd> {
        let (router, tx) = Router::new(RouterConfig::default(), 1024);
        tokio::spawn(router.run());
        tx
    }

    async fn subscribe(
        tx: &mpsc::Sender<RouterCmd>,
        subject: &str,
        queue_group: Option<&str>,
    ) -> SubHandle {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(RouterCmd::Subscribe {
            subject: Box::from(subject),
            queue_group: queue_group.map(Box::from),
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap()
    }

    async fn publish(tx: &mpsc::Sender<RouterCmd>, subject: &str, payload: &[u8]) {
        tx.send(RouterCmd::Publish {
            subject: Box::from(subject),
            payload: Bytes::copy_from_slice(payload),
            reply_to: None,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn fanout_delivery() {
        let tx = setup().await;

        let handle1 = subscribe(&tx, "orders.eu", None).await;
        let handle2 = subscribe(&tx, "orders.eu", None).await;

        publish(&tx, "orders.eu", b"msg1").await;

        match (handle1, handle2) {
            (SubHandle::Fanout { mut rx, .. }, SubHandle::Fanout { rx: mut rx2, .. }) => {
                let d1 = rx.recv().await.unwrap();
                let d2 = rx2.recv().await.unwrap();
                assert_eq!(&*d1.subject, "orders.eu");
                assert_eq!(&*d2.subject, "orders.eu");
                assert_eq!(d1.payload.as_ref(), b"msg1");
            }
            _ => panic!("Expected fanout handles"),
        }
    }

    #[tokio::test]
    async fn queue_group_exactly_once_delivery() {
        // With kanal MPMC dispatch, ordering between members is not guaranteed
        // (whichever member task is idle first takes the message). The invariant
        // is: every published message is delivered to exactly one member.
        let tx = setup().await;

        let handle1 = subscribe(&tx, "jobs.process", Some("workers")).await;
        let handle2 = subscribe(&tx, "jobs.process", Some("workers")).await;

        const N: usize = 20;
        for i in 0..N {
            publish(&tx, "jobs.process", format!("msg{i}").as_bytes()).await;
        }

        let (rx1, rx2) = match (handle1, handle2) {
            (
                SubHandle::QueueMember { rx: rx1, .. },
                SubHandle::QueueMember { rx: rx2, .. },
            ) => (rx1, rx2),
            _ => panic!("Expected queue member handles"),
        };

        let collect = |rx: kanal::AsyncReceiver<Delivery>| async move {
            let mut seen = Vec::new();
            while let Ok(d) = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                rx.recv(),
            )
            .await
            {
                match d {
                    Ok(delivery) => seen.push(
                        String::from_utf8(delivery.payload.to_vec()).unwrap(),
                    ),
                    Err(_) => break,
                }
            }
            seen
        };

        let (got1, got2) = tokio::join!(collect(rx1), collect(rx2));

        let mut all: Vec<String> = got1.iter().chain(got2.iter()).cloned().collect();
        all.sort();
        let mut expected: Vec<String> = (0..N).map(|i| format!("msg{i}")).collect();
        expected.sort();
        assert_eq!(all, expected, "every message delivered exactly once");
        assert!(!got1.is_empty() && !got2.is_empty(), "both members saw work");
    }

    #[tokio::test]
    async fn wildcard_routing() {
        let tx = setup().await;

        let handle = subscribe(&tx, "orders.>", None).await;
        publish(&tx, "orders.eu.created", b"test").await;

        match handle {
            SubHandle::Fanout { mut rx, .. } => {
                let d = rx.recv().await.unwrap();
                assert_eq!(&*d.subject, "orders.eu.created");
            }
            _ => panic!("Expected fanout handle"),
        }
    }

    #[tokio::test]
    async fn idle_queue_member_does_not_block_other_subjects() {
        // A queue group subscriber that never reads must not stall the router.
        // We publish one message to the slow group (fits in the buffer, no
        // blocking), then publish to an unrelated subject and assert the fast
        // subscriber receives it well within the router's single-task budget.
        let tx = setup().await;

        // Slow queue group member — we subscribe but never drain its receiver.
        let slow_handle = subscribe(&tx, "slow.work", Some("workers")).await;
        let _slow_rx = match slow_handle {
            SubHandle::QueueMember { rx, .. } => rx,
            _ => panic!("expected queue member"),
        };

        // Fast fanout subscriber on a completely different subject.
        let fast_handle = subscribe(&tx, "fast.events", None).await;
        let mut fast_rx = match fast_handle {
            SubHandle::Fanout { rx, .. } => rx,
            _ => panic!("expected fanout"),
        };

        // Park a message on the slow queue (buffered, router does not wait).
        publish(&tx, "slow.work", b"stuck").await;

        // Publish to the fast subject and verify it arrives promptly.
        publish(&tx, "fast.events", b"go").await;

        let delivery = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            fast_rx.recv(),
        )
        .await
        .expect("router blocked: fast subject did not deliver in time")
        .expect("fast receiver closed");
        assert_eq!(delivery.payload.as_ref(), b"go");
    }

    #[tokio::test]
    async fn full_queue_channel_blocks_router() {
        // Dual of the previous test: prove that a queue group whose buffer is
        // *full* and whose only member never reads will stall the entire
        // single-task router. This is the known backpressure tradeoff — better
        // than silently dropping, but worth locking in with a regression test.
        let config = RouterConfig {
            broadcast_capacity: 16,
            queue_channel_capacity: 2, // tiny so we fill it in two publishes
        };
        let (router, tx) = Router::new(config, 1024);
        tokio::spawn(router.run());

        // Slow queue member — we hold the receiver but never call recv().
        let slow_handle = subscribe(&tx, "slow.work", Some("workers")).await;
        let _slow_rx = match slow_handle {
            SubHandle::QueueMember { rx, .. } => rx,
            _ => panic!("expected queue member"),
        };

        // Fast subscriber on an unrelated subject — should normally receive
        // promptly, but won't here because the router is stuck.
        let fast_handle = subscribe(&tx, "fast.events", None).await;
        let mut fast_rx = match fast_handle {
            SubHandle::Fanout { rx, .. } => rx,
            _ => panic!("expected fanout"),
        };

        // Fill the shared kanal buffer (capacity 2): these two fit, router
        // does not block yet.
        publish(&tx, "slow.work", b"1").await;
        publish(&tx, "slow.work", b"2").await;

        // Third publish: router calls tx.send(..).await on a full channel,
        // so it parks waiting for a consumer that will never come.
        publish(&tx, "slow.work", b"3").await;

        // While the router is stuck, queue a publish on another subject.
        // It lands in the router's own mpsc but won't be processed.
        publish(&tx, "fast.events", b"go").await;

        // Assert the fast message never arrives — router is blocked.
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            fast_rx.recv(),
        )
        .await;
        assert!(
            result.is_err(),
            "expected router to be blocked by full queue channel, but fast.events delivered: {result:?}"
        );
    }

    #[tokio::test]
    async fn disconnect_cleanup() {
        let tx = setup().await;

        let handle = subscribe(&tx, "test.subject", None).await;
        let sub_id = match &handle {
            SubHandle::Fanout { sub_id, .. } => *sub_id,
            SubHandle::QueueMember { sub_id, .. } => *sub_id,
        };

        // Disconnect.
        tx.send(RouterCmd::Disconnect { sub_id }).await.unwrap();
        // Give router time to process.
        tokio::task::yield_now().await;

        // Now subscribe a new listener to confirm old slot was cleaned up.
        // Publish should go nowhere (no subscribers left).
        // We verify by subscribing fresh and confirming no stale messages.
        let handle2 = subscribe(&tx, "test.subject", None).await;
        publish(&tx, "test.subject", b"after-disconnect").await;

        match handle2 {
            SubHandle::Fanout { mut rx, .. } => {
                let d = rx.recv().await.unwrap();
                assert_eq!(d.payload.as_ref(), b"after-disconnect");
            }
            _ => panic!("Expected fanout handle"),
        }
    }
}
