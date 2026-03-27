use std::path::Path;

use hermes_proto::EventEnvelope;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::debug;

use crate::error::StoreError;
use crate::{DeliveryState, MessageStore, StoredMessage};

// --- Table definitions ---
//
// redb is a typed key-value store. We define our tables with their key/value types.
//
// MESSAGES: message_id (str) -> serialized MessageRecord (bytes)
// DELIVERIES: "consumer_name:message_id" (str) -> serialized DeliveryRecord (bytes)
// CONSUMERS: consumer_name (str) -> serialized ConsumerRecord (bytes)
// SUBJECT_INDEX: "subject:message_id" (str) -> () — for looking up messages by subject

const MESSAGES: TableDefinition<&str, &[u8]> = TableDefinition::new("messages");
const DELIVERIES: TableDefinition<&str, &[u8]> = TableDefinition::new("deliveries");
const CONSUMERS: TableDefinition<&str, &[u8]> = TableDefinition::new("consumers");
const SUBJECT_INDEX: TableDefinition<&str, ()> = TableDefinition::new("subject_index");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Current schema version. Bump when adding tables or changing formats.
const SCHEMA_VERSION: u32 = 1;

/// Record format version byte prepended to serialized records.
const RECORD_VERSION: u8 = 1;

/// A message as stored on disk.
/// We use a simple hand-rolled serialization to avoid pulling in extra deps.
#[derive(Debug, Clone)]
struct MessageRecord {
    id: String,
    subject: String,
    payload: Vec<u8>,
    headers: Vec<(String, String)>,
    timestamp_nanos: i64,
    created_at_ms: u64,
}

impl MessageRecord {
    fn from_envelope(envelope: &EventEnvelope, created_at_ms: u64) -> Self {
        Self {
            id: envelope.id.clone(),
            subject: envelope.subject.clone(),
            payload: envelope.payload.clone(),
            headers: envelope
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            timestamp_nanos: envelope.timestamp_nanos,
            created_at_ms,
        }
    }

    fn to_envelope(&self) -> EventEnvelope {
        EventEnvelope {
            id: self.id.clone(),
            subject: self.subject.clone(),
            payload: self.payload.clone(),
            headers: self.headers.iter().cloned().collect(),
            timestamp_nanos: self.timestamp_nanos,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(RECORD_VERSION);
        write_str(&mut buf, &self.id);
        write_str(&mut buf, &self.subject);
        write_bytes(&mut buf, &self.payload);
        write_u32(&mut buf, self.headers.len() as u32);
        for (k, v) in &self.headers {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }
        write_i64(&mut buf, self.timestamp_nanos);
        write_u64(&mut buf, self.created_at_ms);
        buf
    }

    fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let mut pos = 0;
        let version = data[0];
        pos += 1;

        // Version 0 (legacy): no version byte — re-parse from start.
        // Version 1: current format with version prefix.
        if version != RECORD_VERSION {
            // Attempt legacy parse (version byte is actually start of id length).
            pos = 0;
        }

        let id = read_str(data, &mut pos)?;
        let subject = read_str(data, &mut pos)?;
        let payload = read_bytes(data, &mut pos)?;
        let header_count = read_u32(data, &mut pos)? as usize;
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            let k = read_str(data, &mut pos)?;
            let v = read_str(data, &mut pos)?;
            headers.push((k, v));
        }
        let timestamp_nanos = read_i64(data, &mut pos)?;
        let created_at_ms = read_u64(data, &mut pos)?;
        Some(Self {
            id,
            subject,
            payload,
            headers,
            timestamp_nanos,
            created_at_ms,
        })
    }
}

#[derive(Debug, Clone)]
struct DeliveryRecord {
    state: DeliveryState,
    attempt: u32,
    ack_deadline_ms: u64,
}

impl DeliveryRecord {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(14);
        buf.push(RECORD_VERSION);
        buf.push(self.state.as_u8());
        write_u32(&mut buf, self.attempt);
        write_u64(&mut buf, self.ack_deadline_ms);
        buf
    }

    fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let version = data[0];
        if version == RECORD_VERSION {
            // Version 1: [version, state, attempt(4), deadline(8)] = 14 bytes
            if data.len() < 14 {
                return None;
            }
            let state = DeliveryState::from_u8(data[1])?;
            let mut pos = 2;
            let attempt = read_u32(data, &mut pos)?;
            let ack_deadline_ms = read_u64(data, &mut pos)?;
            Some(Self {
                state,
                attempt,
                ack_deadline_ms,
            })
        } else {
            // Legacy format: [state, attempt(4), deadline(8)] = 13 bytes
            if data.len() < 13 {
                return None;
            }
            let state = DeliveryState::from_u8(data[0])?;
            let mut pos = 1;
            let attempt = read_u32(data, &mut pos)?;
            let ack_deadline_ms = read_u64(data, &mut pos)?;
            Some(Self {
                state,
                attempt,
                ack_deadline_ms,
            })
        }
    }
}

#[derive(Debug, Clone)]
struct ConsumerRecord {
    subject: String,
    queue_groups: Vec<String>,
}

impl ConsumerRecord {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(RECORD_VERSION);
        write_str(&mut buf, &self.subject);
        write_u32(&mut buf, self.queue_groups.len() as u32);
        for qg in &self.queue_groups {
            write_str(&mut buf, qg);
        }
        buf
    }

    fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let mut pos = 0;
        let version = data[0];
        if version == RECORD_VERSION {
            pos = 1; // skip version byte
        }
        let subject = read_str(data, &mut pos)?;

        // Legacy format: single optional queue_group (0 = none, 1 = one).
        // New format: u32 count + N strings.
        // Distinguish by checking: legacy has a single byte 0 or 1 at pos,
        // while new format has a u32 count. Since legacy values are 0 or 1,
        // and we write a u32, we can detect by reading a u32 and checking.
        // However, legacy byte 0 followed by end-of-data = no groups.
        // Legacy byte 1 followed by a string = one group.
        // For backwards compat, detect based on version byte presence.
        if version != RECORD_VERSION {
            // Legacy: single optional queue_group
            let has_qg = *data.get(pos)?;
            pos += 1;
            let queue_groups = if has_qg == 1 {
                vec![read_str(data, &mut pos)?]
            } else {
                vec![]
            };
            return Some(Self {
                subject,
                queue_groups,
            });
        }

        let count = read_u32(data, &mut pos)?;
        let mut queue_groups = Vec::with_capacity(count as usize);
        for _ in 0..count {
            queue_groups.push(read_str(data, &mut pos)?);
        }
        Some(Self {
            subject,
            queue_groups,
        })
    }
}

// --- Serialization helpers ---

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    write_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

fn write_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    write_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let bytes: [u8; 4] = data.get(*pos..*pos + 4)?.try_into().ok()?;
    *pos += 4;
    Some(u32::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let bytes: [u8; 8] = data.get(*pos..*pos + 8)?.try_into().ok()?;
    *pos += 8;
    Some(u64::from_le_bytes(bytes))
}

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    let bytes: [u8; 8] = data.get(*pos..*pos + 8)?.try_into().ok()?;
    *pos += 8;
    Some(i64::from_le_bytes(bytes))
}

fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32(data, pos)? as usize;
    let s = std::str::from_utf8(data.get(*pos..*pos + len)?).ok()?;
    *pos += len;
    Some(s.to_string())
}

fn read_bytes(data: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let len = read_u32(data, pos)? as usize;
    let b = data.get(*pos..*pos + len)?;
    *pos += len;
    Some(b.to_vec())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Separator for composite keys. Using NUL byte which cannot appear in
/// UTF-8 JSON strings, preventing ambiguity when subjects or consumer names
/// contain common delimiters like `:`.
const KEY_SEP: char = '\0';

fn delivery_key(consumer_name: &str, message_id: &str) -> String {
    format!("{consumer_name}{KEY_SEP}{message_id}")
}

fn subject_index_key(subject: &str, message_id: &str) -> String {
    format!("{subject}{KEY_SEP}{message_id}")
}

// --- RedbMessageStore ---

pub struct RedbMessageStore {
    db: Database,
}

impl RedbMessageStore {
    /// Ensure all required tables exist and verify schema version.
    fn init_tables(db: &Database) -> Result<(), StoreError> {
        let txn = db.begin_write()?;
        {
            let _ = txn.open_table(MESSAGES)?;
            let _ = txn.open_table(DELIVERIES)?;
            let _ = txn.open_table(CONSUMERS)?;
            let _ = txn.open_table(SUBJECT_INDEX)?;
            let mut meta = txn.open_table(META)?;

            // Check or set schema version.
            let needs_init = {
                let existing = meta.get("schema_version")?;
                match existing {
                    Some(guard) => {
                        let bytes: &[u8] = guard.value();
                        if bytes.len() >= 4 {
                            let version =
                                u32::from_le_bytes(bytes[..4].try_into().unwrap_or([0; 4]));
                            if version > SCHEMA_VERSION {
                                return Err(StoreError::InvalidState(version as u8));
                            }
                        }
                        false
                    }
                    None => true,
                }
            };
            if needs_init {
                meta.insert("schema_version", SCHEMA_VERSION.to_le_bytes().as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Open or create a store at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db = Database::create(path.as_ref())?;
        Self::init_tables(&db)?;
        Ok(Self { db })
    }

    /// Open an in-memory store (for tests).
    #[cfg(test)]
    pub fn open_temporary() -> Result<Self, StoreError> {
        let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;
        Self::init_tables(&db)?;
        Ok(Self { db })
    }
}

impl MessageStore for RedbMessageStore {
    fn persist(&self, envelope: &EventEnvelope) -> Result<(), StoreError> {
        let record = MessageRecord::from_envelope(envelope, now_ms());
        let serialized = record.serialize();

        let txn = self.db.begin_write()?;
        {
            let mut messages = txn.open_table(MESSAGES)?;
            messages.insert(envelope.id.as_str(), serialized.as_slice())?;

            let mut idx = txn.open_table(SUBJECT_INDEX)?;
            let key = subject_index_key(&envelope.subject, &envelope.id);
            idx.insert(key.as_str(), ())?;
        }
        txn.commit()?;

        debug!(
            id = envelope.id,
            subject = envelope.subject,
            "message persisted"
        );
        Ok(())
    }

    fn register_consumer(
        &self,
        consumer_name: &str,
        subject: &str,
        queue_groups: &[String],
    ) -> Result<(), StoreError> {
        let record = ConsumerRecord {
            subject: subject.to_string(),
            queue_groups: queue_groups.to_vec(),
        };

        let txn = self.db.begin_write()?;
        {
            let mut consumers = txn.open_table(CONSUMERS)?;
            consumers.insert(consumer_name, record.serialize().as_slice())?;
        }
        txn.commit()?;

        debug!(consumer_name, subject, "consumer registered");
        Ok(())
    }

    fn fetch_pending(
        &self,
        consumer_name: &str,
        limit: u32,
    ) -> Result<Vec<StoredMessage>, StoreError> {
        let txn = self.db.begin_read()?;

        // Find the consumer's subject.
        let consumers = txn.open_table(CONSUMERS)?;
        let consumer_data = consumers
            .get(consumer_name)?
            .ok_or_else(|| StoreError::ConsumerNotFound(consumer_name.to_string()))?;
        let consumer = ConsumerRecord::deserialize(consumer_data.value())
            .ok_or(StoreError::InvalidState(255))?;

        let messages = txn.open_table(MESSAGES)?;
        let deliveries = txn.open_table(DELIVERIES)?;
        let idx = txn.open_table(SUBJECT_INDEX)?;

        let prefix = format!("{}{KEY_SEP}", consumer.subject);
        let mut result = Vec::new();

        let range = idx.range(prefix.as_str()..)?;
        for entry in range {
            let entry = entry?;
            let key = entry.0.value();

            // Stop if we've left the prefix range.
            if !key.starts_with(&prefix) {
                break;
            }

            if result.len() >= limit as usize {
                break;
            }

            let message_id = &key[prefix.len()..];
            let dk = delivery_key(consumer_name, message_id);

            // Skip if already has a delivery record (delivered, acked, or dead-lettered).
            if deliveries.get(dk.as_str())?.is_some() {
                continue;
            }

            // Fetch the message.
            if let Some(msg_data) = messages.get(message_id)? {
                let bytes: &[u8] = msg_data.value();
                if let Some(record) = MessageRecord::deserialize(bytes) {
                    result.push(StoredMessage {
                        envelope: record.to_envelope(),
                        attempt: 1,
                    });
                }
            }
        }

        Ok(result)
    }

    fn mark_delivered(
        &self,
        message_id: &str,
        consumer_name: &str,
        ack_deadline_ms: u64,
    ) -> Result<(), StoreError> {
        let dk = delivery_key(consumer_name, message_id);
        let record = DeliveryRecord {
            state: DeliveryState::Delivered,
            attempt: 1,
            ack_deadline_ms,
        };

        let txn = self.db.begin_write()?;
        {
            let mut deliveries = txn.open_table(DELIVERIES)?;
            deliveries.insert(dk.as_str(), record.serialize().as_slice())?;
        }
        txn.commit()?;

        Ok(())
    }

    fn ack(&self, message_id: &str, consumer_name: &str) -> Result<(), StoreError> {
        let dk = delivery_key(consumer_name, message_id);

        let txn = self.db.begin_write()?;
        {
            let mut deliveries = txn.open_table(DELIVERIES)?;
            let existing = deliveries
                .get(dk.as_str())?
                .ok_or_else(|| StoreError::MessageNotFound(message_id.to_string()))?;
            let mut record = DeliveryRecord::deserialize(existing.value())
                .ok_or(StoreError::InvalidState(255))?;
            drop(existing);

            record.state = DeliveryState::Acked;
            deliveries.insert(dk.as_str(), record.serialize().as_slice())?;
        }
        txn.commit()?;

        debug!(message_id, consumer_name, "message acked");
        Ok(())
    }

    fn nack(&self, message_id: &str, consumer_name: &str, requeue: bool) -> Result<(), StoreError> {
        let dk = delivery_key(consumer_name, message_id);

        let txn = self.db.begin_write()?;
        {
            let mut deliveries = txn.open_table(DELIVERIES)?;
            let existing = deliveries
                .get(dk.as_str())?
                .ok_or_else(|| StoreError::MessageNotFound(message_id.to_string()))?;
            let record = DeliveryRecord::deserialize(existing.value())
                .ok_or(StoreError::InvalidState(255))?;
            drop(existing);

            if requeue {
                // Remove the delivery record entirely so fetch_pending picks it up again.
                deliveries.remove(dk.as_str())?;
                debug!(message_id, consumer_name, "message nacked, requeued");
            } else {
                // Dead-letter.
                let dead = DeliveryRecord {
                    state: DeliveryState::DeadLettered,
                    attempt: record.attempt,
                    ack_deadline_ms: 0,
                };
                deliveries.insert(dk.as_str(), dead.serialize().as_slice())?;
                debug!(message_id, consumer_name, "message nacked, dead-lettered");
            }
        }
        txn.commit()?;

        Ok(())
    }

    fn fetch_expired(
        &self,
        consumer_name: &str,
        now_ms: u64,
        limit: u32,
    ) -> Result<Vec<StoredMessage>, StoreError> {
        let txn = self.db.begin_read()?;
        let deliveries = txn.open_table(DELIVERIES)?;
        let messages = txn.open_table(MESSAGES)?;

        let prefix = format!("{consumer_name}{KEY_SEP}");
        let mut result = Vec::new();

        let range = deliveries.range(prefix.as_str()..)?;
        for entry in range {
            let entry = entry?;
            let key = entry.0.value();

            if !key.starts_with(&prefix) {
                break;
            }

            if result.len() >= limit as usize {
                break;
            }

            let record = match DeliveryRecord::deserialize(entry.1.value()) {
                Some(r) => r,
                None => continue,
            };

            // Only pick up delivered (not acked, not dead-lettered) with expired deadline.
            if record.state != DeliveryState::Delivered || record.ack_deadline_ms > now_ms {
                continue;
            }

            let message_id = &key[prefix.len()..];
            if let Some(msg_data) = messages.get(message_id)?
                && let Some(msg) = MessageRecord::deserialize(msg_data.value())
            {
                result.push(StoredMessage {
                    envelope: msg.to_envelope(),
                    attempt: record.attempt.saturating_add(1),
                });
            }
        }

        Ok(result)
    }

    fn gc_acked(&self, older_than_ms: u64) -> Result<u64, StoreError> {
        let txn = self.db.begin_write()?;
        let mut removed: u64 = 0;

        {
            let mut messages = txn.open_table(MESSAGES)?;
            let mut idx = txn.open_table(SUBJECT_INDEX)?;
            let mut deliveries = txn.open_table(DELIVERIES)?;

            // Phase 1: Single pass over deliveries → build per-message status.
            // Key format is "consumer_name:message_id".
            // We track: message_id → (has_deliveries, all_terminal).
            let mut msg_status: std::collections::HashMap<String, bool> =
                std::collections::HashMap::new();
            let mut del_keys_by_msg: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();

            for entry in deliveries.range::<&str>(..)? {
                let entry = entry?;
                let key = entry.0.value().to_string();
                // Extract message_id from "consumer_name\0message_id".
                let message_id = match key.rsplit_once(KEY_SEP) {
                    Some((_, mid)) => mid.to_string(),
                    None => continue,
                };

                let terminal = DeliveryRecord::deserialize(entry.1.value())
                    .map(|dr| {
                        dr.state == DeliveryState::Acked || dr.state == DeliveryState::DeadLettered
                    })
                    .unwrap_or(false);

                let all_terminal = msg_status.entry(message_id.clone()).or_insert(true);
                if !terminal {
                    *all_terminal = false;
                }

                del_keys_by_msg.entry(message_id).or_default().push(key);
            }

            // Phase 2: Scan messages, check age + delivery status.
            let msg_range: Vec<_> = {
                messages
                    .range::<&str>(..)?
                    .filter_map(|e| {
                        let e = e.ok()?;
                        let id = e.0.value().to_string();
                        let record = MessageRecord::deserialize(e.1.value())?;
                        Some((id, record))
                    })
                    .collect()
            };

            for (id, record) in &msg_range {
                if record.created_at_ms > older_than_ms {
                    continue;
                }

                // GC-eligible: all delivery records are terminal.
                let all_terminal = msg_status.get(id).copied().unwrap_or(false);
                if !all_terminal {
                    continue;
                }

                messages.remove(id.as_str())?;
                let idx_key = subject_index_key(&record.subject, id);
                idx.remove(idx_key.as_str())?;

                if let Some(del_keys) = del_keys_by_msg.remove(id) {
                    for dk in del_keys {
                        deliveries.remove(dk.as_str())?;
                    }
                }

                removed = removed.saturating_add(1);
            }
        }

        txn.commit()?;

        if removed > 0 {
            debug!(removed, "gc completed");
        }

        Ok(removed)
    }

    fn list_consumers(&self) -> Result<Vec<String>, StoreError> {
        let txn = self.db.begin_read()?;
        let consumers = txn.open_table(CONSUMERS)?;

        let mut names = Vec::new();
        for entry in consumers.range::<&str>(..)? {
            let entry = entry?;
            names.push(entry.0.value().to_string());
        }

        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_envelope(id: &str, subject: &str) -> EventEnvelope {
        EventEnvelope {
            id: id.to_string(),
            subject: subject.to_string(),
            payload: vec![1, 2, 3],
            headers: Default::default(),
            timestamp_nanos: 42,
        }
    }

    #[test]
    fn test_persist_and_fetch_pending() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store
            .persist(&test_envelope("msg-2", "orders::Created"))
            .unwrap();

        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].envelope.id, "msg-1");
        assert_eq!(pending[1].envelope.id, "msg-2");
    }

    #[test]
    fn test_mark_delivered_excludes_from_pending() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store.mark_delivered("msg-1", "worker-1", u64::MAX).unwrap();

        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_ack() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store.mark_delivered("msg-1", "worker-1", u64::MAX).unwrap();
        store.ack("msg-1", "worker-1").unwrap();

        // Should not show up in pending or expired.
        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert!(pending.is_empty());
        let expired = store.fetch_expired("worker-1", u64::MAX, 10).unwrap();
        assert!(expired.is_empty());
    }

    #[test]
    fn test_nack_requeue() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store.mark_delivered("msg-1", "worker-1", u64::MAX).unwrap();
        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert_eq!(pending.len(), 0);
        store.nack("msg-1", "worker-1", true).unwrap();

        // Should show up in pending again.
        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn test_nack_dead_letter() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store.mark_delivered("msg-1", "worker-1", u64::MAX).unwrap();
        store.nack("msg-1", "worker-1", false).unwrap();

        // Should not show up anywhere.
        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert!(pending.is_empty());
        let expired = store.fetch_expired("worker-1", u64::MAX, 10).unwrap();
        assert!(expired.is_empty());
    }

    #[test]
    fn test_fetch_expired() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store.mark_delivered("msg-1", "worker-1", 1000).unwrap();

        // Not expired yet.
        let expired = store.fetch_expired("worker-1", 999, 10).unwrap();
        assert!(expired.is_empty());

        // Now expired.
        let expired = store.fetch_expired("worker-1", 1001, 10).unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].attempt, 2); // incremented
    }

    #[test]
    fn test_gc_acked() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store.mark_delivered("msg-1", "worker-1", u64::MAX).unwrap();
        store.ack("msg-1", "worker-1").unwrap();

        // GC with a future threshold should remove it.
        let removed = store.gc_acked(u64::MAX).unwrap();
        assert_eq!(removed, 1);

        // Should be gone.
        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_list_consumers() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .register_consumer("worker-2", "orders::Shipped", &[])
            .unwrap();

        let mut consumers = store.list_consumers().unwrap();
        consumers.sort();
        assert_eq!(consumers, vec!["worker-1", "worker-2"]);
    }

    #[test]
    fn test_different_subjects_isolated() {
        let store = RedbMessageStore::open_temporary().unwrap();

        store
            .register_consumer("worker-1", "orders::Created", &[])
            .unwrap();
        store
            .persist(&test_envelope("msg-1", "orders::Created"))
            .unwrap();
        store
            .persist(&test_envelope("msg-2", "orders::Shipped"))
            .unwrap();

        // worker-1 only sees orders::Created.
        let pending = store.fetch_pending("worker-1", 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].envelope.subject, "orders::Created");
    }

    #[test]
    fn test_message_record_roundtrip() {
        let envelope = EventEnvelope {
            id: "test-id".into(),
            subject: "test::Subject".into(),
            payload: vec![1, 2, 3, 4, 5],
            headers: [("key".to_string(), "value".to_string())].into(),
            timestamp_nanos: 123456789,
        };
        let record = MessageRecord::from_envelope(&envelope, 999);
        let serialized = record.serialize();
        let deserialized = MessageRecord::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.id, "test-id");
        assert_eq!(deserialized.subject, "test::Subject");
        assert_eq!(deserialized.payload, vec![1, 2, 3, 4, 5]);
        assert_eq!(deserialized.headers, vec![("key".into(), "value".into())]);
        assert_eq!(deserialized.timestamp_nanos, 123456789);
        assert_eq!(deserialized.created_at_ms, 999);
    }
}
