use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use tracing::{debug, trace};

use crate::trie::SlotId;

/// Unique identifier for a subscriber connection.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SubId(pub u64);

/// Identifier for a subscribe session — typically one per gRPC subscribe
/// stream. All subscribers created under a session are cleaned up in one
/// shot when the session ends, instead of the caller tracking SubIds and
/// firing N [`RouterCmd::Disconnect`](crate::router::RouterCmd::Disconnect)
/// commands.
///
/// Allocated by the caller (the server assigns a fresh value from a
/// process-wide counter, tests mint them locally). Opaque to the router.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SessionId(pub u64);

/// A message routed through the broker.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// The subject the message was published to (e.g. `orders.eu.created`).
    pub subject: Box<str>,
    /// Raw message payload.
    pub payload: Bytes,
    /// Monotonically increasing sequence number assigned by the router.
    pub sequence: u64,
    /// Optional reply-to subject for request/reply patterns.
    pub reply_to: Option<Box<str>>,
}

/// `(subject, queue_group)` index key. `None` queue group = fanout slot.
type IndexKey = (Box<str>, Option<Box<str>>);

/// A routing slot attached to trie nodes.
///
/// Channels carry `Arc<Delivery>` so that every receiver-side clone is a
/// single atomic refcount bump — no heap allocation per subscriber, which
/// was the dominant fanout cost for large slots.
pub enum Slot {
    /// Fanout: router calls `sender.send` directly. `broadcast::send` is a
    /// synchronous non-blocking call that writes to the internal ring
    /// (slow receivers get a `Lagged` error on their next recv) and walks
    /// the waker list inline. No dispatcher task, no extra hop, no await.
    Broadcast {
        /// Template sender kept so new subscribers can `.subscribe()` a receiver.
        sender: broadcast::Sender<Arc<Delivery>>,
        /// Explicitly tracked subscriber count. `broadcast::Sender::receiver_count()`
        /// lags our view (grpc tasks keep receivers alive briefly after router
        /// marks the sub removed), so we count ourselves for cleanup decisions.
        sub_count: usize,
    },
    /// Load-balanced: one shared MPMC channel, members compete to receive.
    /// Whichever member task is idle first takes the delivery — natural
    /// work-stealing, no cursor bookkeeping, no head-of-line blocking.
    QueueGroup {
        tx: kanal::AsyncSender<Arc<Delivery>>,
        /// Template receiver kept so new subscribers can clone their own handle.
        rx: kanal::AsyncReceiver<Arc<Delivery>>,
        /// Member SubIds, only used for disconnect bookkeeping.
        member_ids: Vec<SubId>,
    },
}

/// Handle returned to the subscriber so it can receive deliveries.
pub enum SubHandle {
    /// Fanout subscription — receives a clone of every matching message.
    Fanout {
        /// Unique subscriber identifier.
        sub_id: SubId,
        /// Broadcast receiver for incoming deliveries.
        rx: broadcast::Receiver<Arc<Delivery>>,
    },
    /// Queue group member — shares a kanal MPMC receiver with its peers.
    /// Each delivery is taken by exactly one member; idle members pick up
    /// work first, so slow peers don't hold up the group.
    QueueMember {
        /// Unique subscriber identifier.
        sub_id: SubId,
        /// Kanal async receiver cloned from the group's shared channel.
        rx: kanal::AsyncReceiver<Arc<Delivery>>,
    },
}

/// Manages all routing slots and the mapping from (subject, queue group) to [`SlotId`].
///
/// Handles subscriber registration, slot lifecycle, and cleanup on disconnect.
/// Used exclusively by the [`Router`](crate::router::Router).
#[derive(Default)]
pub struct SlotMap {
    /// Hot path: looked up per publish. Uses identity hash on `SlotId(u64)`.
    slots: FxHashMap<SlotId, Slot>,
    /// `(subject, Option<queue_group>) → SlotId`. Cold path — only touched on
    /// subscribe/unsubscribe, so default string hash is fine.
    index: HashMap<IndexKey, SlotId>,
    /// Reverse of `index`: `SlotId → IndexKey`. Lets `remove_subscriber` drop
    /// the index entry in O(1) instead of a full `index.retain` scan.
    slot_keys: FxHashMap<SlotId, IndexKey>,
    /// `SubId → list of (SlotId, subject)` for cleanup on disconnect.
    /// Cold path, identity hash gives a small win for the disconnect sweep.
    sub_slots: FxHashMap<SubId, Vec<(SlotId, Box<str>)>>,
    /// `SessionId → list of SubIds` — lets a single `end_session` drop every
    /// subscriber created under that session.
    session_subs: FxHashMap<SessionId, Vec<SubId>>,
    next_slot_id: u64,
    next_sub_id: u64,
}

impl SlotMap {
    /// Create an empty slot map.
    pub fn new() -> Self {
        Self::default()
    }

    fn alloc_slot_id(&mut self) -> SlotId {
        let id = SlotId(self.next_slot_id);
        self.next_slot_id += 1;
        id
    }

    fn alloc_sub_id(&mut self) -> SubId {
        let id = SubId(self.next_sub_id);
        self.next_sub_id += 1;
        id
    }

    /// Record a newly-created subscriber in both the per-sub reverse index
    /// (used for single-sub `remove_subscriber`) and the per-session index
    /// (used for `end_session`).
    fn register_sub(
        &mut self,
        sub_id: SubId,
        session_id: SessionId,
        slot_id: SlotId,
        subject: &str,
    ) {
        self.sub_slots
            .entry(sub_id)
            .or_default()
            .push((slot_id, Box::from(subject)));
        self.session_subs.entry(session_id).or_default().push(sub_id);
    }

    /// Subscribe to a fanout (broadcast) slot. Creates the slot if it doesn't exist.
    /// Returns `(SubHandle, Option<SlotId>)` — `SlotId` is `Some` if a new slot was
    /// created (caller must insert it into the trie).
    pub fn subscribe_fanout(
        &mut self,
        subject: &str,
        session_id: SessionId,
        broadcast_capacity: usize,
    ) -> (SubHandle, Option<SlotId>) {
        let key: IndexKey = (Box::from(subject), None);
        let sub_id = self.alloc_sub_id();

        if let Some(&slot_id) = self.index.get(&key) {
            // Existing broadcast slot — just clone a receiver.
            if let Some(Slot::Broadcast {
                sender, sub_count, ..
            }) = self.slots.get_mut(&slot_id)
            {
                let rx = sender.subscribe();
                *sub_count += 1;
                let count_snapshot = *sub_count;
                self.register_sub(sub_id, session_id, slot_id, subject);
                debug!(
                    subject,
                    slot_id = slot_id.0,
                    sub_id = sub_id.0,
                    sub_count = count_snapshot,
                    "joined existing fanout slot"
                );
                return (SubHandle::Fanout { sub_id, rx }, None);
            }
        }

        // New fanout slot. Router publishes by calling `sender.send` directly —
        // tokio broadcast is synchronous non-blocking, so there is no need for
        // an intermediate dispatcher task or mpsc channel.
        let slot_id = self.alloc_slot_id();
        let (sender, rx) = broadcast::channel(broadcast_capacity);

        self.slots.insert(
            slot_id,
            Slot::Broadcast {
                sender,
                sub_count: 1,
            },
        );
        self.index.insert(key.clone(), slot_id);
        self.slot_keys.insert(slot_id, key);
        self.register_sub(sub_id, session_id, slot_id, subject);

        debug!(
            subject,
            slot_id = slot_id.0,
            sub_id = sub_id.0,
            "created new fanout slot"
        );

        (SubHandle::Fanout { sub_id, rx }, Some(slot_id))
    }

    /// Subscribe to a queue-group slot. Creates the slot if it doesn't exist.
    /// Returns `(SubHandle, Option<SlotId>)`.
    pub fn subscribe_queue_group(
        &mut self,
        subject: &str,
        group: &str,
        session_id: SessionId,
        channel_capacity: usize,
    ) -> (SubHandle, Option<SlotId>) {
        let key: IndexKey = (Box::from(subject), Some(Box::from(group)));
        let sub_id = self.alloc_sub_id();

        if let Some(&slot_id) = self.index.get(&key)
            && let Some(Slot::QueueGroup { rx, member_ids, .. }) = self.slots.get_mut(&slot_id)
        {
            let member_rx = rx.clone();
            member_ids.push(sub_id);
            let member_count = member_ids.len();
            self.register_sub(sub_id, session_id, slot_id, subject);
            debug!(
                subject,
                group,
                slot_id = slot_id.0,
                sub_id = sub_id.0,
                member_count,
                "joined existing queue group"
            );
            return (
                SubHandle::QueueMember {
                    sub_id,
                    rx: member_rx,
                },
                None,
            );
        }

        // New queue group slot — one shared MPMC channel for all members.
        let slot_id = self.alloc_slot_id();
        let (tx, rx) = kanal::bounded_async::<Arc<Delivery>>(channel_capacity);
        let member_rx = rx.clone();
        self.slots.insert(
            slot_id,
            Slot::QueueGroup {
                tx,
                rx,
                member_ids: vec![sub_id],
            },
        );
        self.index.insert(key.clone(), slot_id);
        self.slot_keys.insert(slot_id, key);
        self.register_sub(sub_id, session_id, slot_id, subject);

        debug!(
            subject,
            group,
            slot_id = slot_id.0,
            sub_id = sub_id.0,
            "created new queue group slot"
        );

        (
            SubHandle::QueueMember {
                sub_id,
                rx: member_rx,
            },
            Some(slot_id),
        )
    }

    /// Remove a subscriber from all its slots. Returns slot IDs that became empty
    /// (caller should remove them from the trie).
    pub fn remove_subscriber(&mut self, sub_id: SubId) -> Vec<(SlotId, Box<str>)> {
        let mut empty_slots = Vec::new();

        if let Some(entries) = self.sub_slots.remove(&sub_id) {
            debug!(
                sub_id = sub_id.0,
                slot_count = entries.len(),
                "removing subscriber from all slots"
            );

            for (slot_id, subject) in entries {
                let should_remove = match self.slots.get_mut(&slot_id) {
                    Some(Slot::Broadcast { sub_count, .. }) => {
                        *sub_count = sub_count.saturating_sub(1);
                        *sub_count == 0
                    }
                    Some(Slot::QueueGroup { member_ids, .. }) => {
                        member_ids.retain(|id| *id != sub_id);
                        member_ids.is_empty()
                    }
                    None => false,
                };

                trace!(
                    sub_id = sub_id.0,
                    slot_id = slot_id.0,
                    subject = %subject,
                    will_remove = should_remove,
                    "processed slot removal"
                );

                if should_remove {
                    self.slots.remove(&slot_id);
                    if let Some(key) = self.slot_keys.remove(&slot_id) {
                        self.index.remove(&key);
                    }
                    empty_slots.push((slot_id, subject));
                }
            }
        }

        empty_slots
    }

    /// Drop every subscriber created under `session_id`. Returns the aggregated
    /// list of `(SlotId, subject)` that became empty so the caller can prune
    /// them from the trie. Equivalent to calling [`remove_subscriber`] once per
    /// [`SubId`] registered under the session, but avoids forcing the caller to
    /// track SubIds itself.
    ///
    /// [`remove_subscriber`]: Self::remove_subscriber
    pub fn end_session(&mut self, session_id: SessionId) -> Vec<(SlotId, Box<str>)> {
        let Some(sub_ids) = self.session_subs.remove(&session_id) else {
            return Vec::new();
        };

        debug!(
            session_id = session_id.0,
            sub_count = sub_ids.len(),
            "ending session"
        );

        let mut empty_slots = Vec::new();
        for sub_id in sub_ids {
            empty_slots.extend(self.remove_subscriber(sub_id));
        }
        empty_slots
    }

    /// Get a slot by ID.
    pub fn get_mut(&mut self, slot_id: &SlotId) -> Option<&mut Slot> {
        self.slots.get_mut(slot_id)
    }
}
