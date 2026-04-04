use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};

use crate::trie::SlotId;

/// Unique identifier for a subscriber connection.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SubId(pub u64);

/// A message routed through the broker.
#[derive(Clone, Debug)]
pub struct Delivery {
    pub subject: Box<str>,
    pub payload: Bytes,
    pub sequence: u64,
    pub reply_to: Option<Box<str>>,
}

/// A member of a queue group.
pub struct QueueMember {
    pub sub_id: SubId,
    pub tx: mpsc::Sender<Delivery>,
}

/// A routing slot attached to trie nodes.
pub enum Slot {
    /// Fanout: one send, all receivers get it.
    Broadcast {
        sender: broadcast::Sender<Delivery>,
        /// Explicitly tracked subscriber count (don't rely on receiver_count()).
        sub_count: usize,
    },
    /// Load-balanced: router picks one member round-robin.
    QueueGroup {
        members: Vec<QueueMember>,
        next: usize,
    },
}

/// Handle returned to the subscriber so it can receive deliveries.
pub enum SubHandle {
    Fanout {
        sub_id: SubId,
        rx: broadcast::Receiver<Delivery>,
    },
    QueueMember {
        sub_id: SubId,
        rx: mpsc::Receiver<Delivery>,
    },
}

/// Manages all slots and the mapping from (subject, group) to SlotId.
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
        let (tx, rx) = mpsc::channel(channel_capacity);

        if let Some(&slot_id) = self.index.get(&key)
            && let Some(Slot::QueueGroup { members, .. }) = self.slots.get_mut(&slot_id)
        {
            members.push(QueueMember { sub_id, tx });
            self.sub_slots
                .entry(sub_id)
                .or_default()
                .push((slot_id, Box::from(subject)));
            return (SubHandle::QueueMember { sub_id, rx }, None);
        }

        // New queue group slot.
        let slot_id = self.alloc_slot_id();
        self.slots.insert(
            slot_id,
            Slot::QueueGroup {
                members: vec![QueueMember { sub_id, tx }],
                next: 0,
            },
        );
        self.index.insert(key, slot_id);
        self.sub_slots
            .entry(sub_id)
            .or_default()
            .push((slot_id, Box::from(subject)));

        (SubHandle::QueueMember { sub_id, rx }, Some(slot_id))
    }

    /// Remove a subscriber from all its slots. Returns slot IDs that became empty
    /// (caller should remove them from the trie).
    pub fn remove_subscriber(&mut self, sub_id: SubId) -> Vec<(SlotId, Box<str>)> {
        let mut empty_slots = Vec::new();

        if let Some(entries) = self.sub_slots.remove(&sub_id) {
            for (slot_id, subject) in entries {
                let should_remove = match self.slots.get_mut(&slot_id) {
                    Some(Slot::Broadcast { sub_count, .. }) => {
                        *sub_count = sub_count.saturating_sub(1);
                        *sub_count == 0
                    }
                    Some(Slot::QueueGroup { members, .. }) => {
                        members.retain(|m| m.sub_id != sub_id);
                        members.is_empty()
                    }
                    None => false,
                };

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
