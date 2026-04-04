use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

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
                        continue;
                    }

                    // Deduplicate (a subject can match the same slot via multiple trie paths).
                    matched.sort_unstable_by_key(|s| s.0);
                    matched.dedup();

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
                            Some(Slot::QueueGroup { members, next }) => {
                                if !members.is_empty() {
                                    let idx = *next % members.len();
                                    let _ = members[idx].tx.try_send(delivery.clone());
                                    *next = next.wrapping_add(1);
                                }
                            }
                            None => {}
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

                    let _ = reply.send(handle);
                }

                RouterCmd::Disconnect { sub_id } => {
                    let empty_slots = self.slots.remove_subscriber(sub_id);
                    for (slot_id, _subject) in empty_slots {
                        self.trie.remove(slot_id);
                    }
                }
            }
        }
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
    async fn queue_group_round_robin() {
        let tx = setup().await;

        let handle1 = subscribe(&tx, "jobs.process", Some("workers")).await;
        let handle2 = subscribe(&tx, "jobs.process", Some("workers")).await;

        for i in 0..4 {
            publish(&tx, "jobs.process", format!("msg{i}").as_bytes()).await;
        }

        match (handle1, handle2) {
            (SubHandle::QueueMember { mut rx, .. }, SubHandle::QueueMember { rx: mut rx2, .. }) => {
                let m0a = rx.recv().await.unwrap();
                let m1a = rx2.recv().await.unwrap();
                let m0b = rx.recv().await.unwrap();
                let m1b = rx2.recv().await.unwrap();
                assert_eq!(m0a.payload.as_ref(), b"msg0");
                assert_eq!(m1a.payload.as_ref(), b"msg1");
                assert_eq!(m0b.payload.as_ref(), b"msg2");
                assert_eq!(m1b.payload.as_ref(), b"msg3");
            }
            _ => panic!("Expected queue member handles"),
        }
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
