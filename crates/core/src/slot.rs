use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{debug, trace};

use crate::trie::SlotId;

/// Unique identifier for a subscriber connection.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SubId(pub u64);

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

/// A routing slot attached to trie nodes.
pub enum Slot {
    /// Fanout: one send, all receivers get it.
    Broadcast {
        sender: broadcast::Sender<Delivery>,
        /// Explicitly tracked subscriber count (don't rely on receiver_count()).
        sub_count: usize,
    },
    /// Load-balanced: one shared MPMC channel, members compete to receive.
    /// Whichever member task is idle first takes the delivery — natural
    /// work-stealing, no cursor bookkeeping, no head-of-line blocking.
    QueueGroup {
        tx: kanal::AsyncSender<Delivery>,
        /// Template receiver kept so new subscribers can clone their own handle.
        rx: kanal::AsyncReceiver<Delivery>,
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
        rx: broadcast::Receiver<Delivery>,
    },
    /// Queue group member — shares a kanal MPMC receiver with its peers.
    /// Each delivery is taken by exactly one member; idle members pick up
    /// work first, so slow peers don't hold up the group.
    QueueMember {
        /// Unique subscriber identifier.
        sub_id: SubId,
        /// Kanal async receiver cloned from the group's shared channel.
        rx: kanal::AsyncReceiver<Delivery>,
    },
}

/// Manages all routing slots and the mapping from (subject, queue group) to [`SlotId`].
///
/// Handles subscriber registration, slot lifecycle, and cleanup on disconnect.
/// Used exclusively by the [`Router`](crate::router::Router).
#[derive(Default)]
pub struct SlotMap {
    slots: HashMap<SlotId, Slot>,
    /// (subject, queue_group_or_empty) → SlotId
    index: HashMap<(Box<str>, Box<str>), SlotId>,
    /// Reverse: SubId → list of (SlotId, subject) for cleanup on disconnect.
    sub_slots: HashMap<SubId, Vec<(SlotId, Box<str>)>>,
    next_slot_id: u64,
    next_sub_id: u64,
}

impl SlotMap {
    /// Create an empty slot map.
    pub fn new() -> Self {
        Self {
            slots: HashMap::new(),
            index: HashMap::new(),
            sub_slots: HashMap::new(),
            next_slot_id: 0,
            next_sub_id: 0,
        }
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

    /// Subscribe to a fanout (broadcast) slot. Creates the slot if it doesn't exist.
    /// Returns `(SubHandle, Option<SlotId>)` — SlotId is Some if a new slot was created
    /// (caller must insert it into the trie).
    pub fn subscribe_fanout(
        &mut self,
        subject: &str,
        broadcast_capacity: usize,
    ) -> (SubHandle, Option<SlotId>) {
        let key = (Box::from(subject), Box::from(""));
        let sub_id = self.alloc_sub_id();

        if let Some(&slot_id) = self.index.get(&key) {
            // Existing broadcast slot — just clone a receiver.
            if let Some(Slot::Broadcast { sender, sub_count }) = self.slots.get_mut(&slot_id) {
                let rx = sender.subscribe();
                *sub_count += 1;
                self.sub_slots
                    .entry(sub_id)
                    .or_default()
                    .push((slot_id, Box::from(subject)));
                debug!(
                    subject,
                    slot_id = slot_id.0,
                    sub_id = sub_id.0,
                    sub_count = *sub_count,
                    "joined existing fanout slot"
                );
                return (SubHandle::Fanout { sub_id, rx }, None);
            }
        }

        // New broadcast slot.
        let slot_id = self.alloc_slot_id();
        let (sender, rx) = broadcast::channel(broadcast_capacity);
        self.slots.insert(
            slot_id,
            Slot::Broadcast {
                sender,
                sub_count: 1,
            },
        );
        self.index.insert(key, slot_id);
        self.sub_slots
            .entry(sub_id)
            .or_default()
            .push((slot_id, Box::from(subject)));

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
        channel_capacity: usize,
    ) -> (SubHandle, Option<SlotId>) {
        let key = (Box::from(subject), Box::from(group));
        let sub_id = self.alloc_sub_id();

        if let Some(&slot_id) = self.index.get(&key)
            && let Some(Slot::QueueGroup {
                rx, member_ids, ..
            }) = self.slots.get_mut(&slot_id)
        {
            let member_rx = rx.clone();
            member_ids.push(sub_id);
            self.sub_slots
                .entry(sub_id)
                .or_default()
                .push((slot_id, Box::from(subject)));
            debug!(
                subject,
                group,
                slot_id = slot_id.0,
                sub_id = sub_id.0,
                member_count = member_ids.len(),
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
        let (tx, rx) = kanal::bounded_async::<Delivery>(channel_capacity);
        let member_rx = rx.clone();
        self.slots.insert(
            slot_id,
            Slot::QueueGroup {
                tx,
                rx,
                member_ids: vec![sub_id],
            },
        );
        self.index.insert(key, slot_id);
        self.sub_slots
            .entry(sub_id)
            .or_default()
            .push((slot_id, Box::from(subject)));

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
                    // Find and remove from index.
                    self.index.retain(|_, v| *v != slot_id);
                    empty_slots.push((slot_id, subject));
                }
            }
        }

        empty_slots
    }

    /// Get a slot by ID.
    pub fn get_mut(&mut self, slot_id: &SlotId) -> Option<&mut Slot> {
        self.slots.get_mut(slot_id)
    }
}
