use std::sync::Arc;
use std::time::Duration;

use hermes_core::Subject;
use hermes_proto::{DurableServerMessage, Redelivery};
use hermes_store::MessageStore;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::broker::BrokerEngine;

/// Spawn the redelivery loop that checks for expired messages and re-delivers them.
/// The loop stops when `cancel` is triggered.
pub fn spawn_redelivery_loop(
    engine: Arc<BrokerEngine>,
    interval_secs: u64,
    max_attempts: u32,
    batch_size: u32,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await; // skip the first immediate tick

        loop {
            select! {
                _ = interval.tick() => {}
                () = cancel.cancelled() => break,
            }

            let store = match engine.store() {
                Some(s) => s,
                None => continue,
            };

            let consumers = match store.list_consumers() {
                Ok(c) => c,
                Err(e) => {
                    error!("redelivery: failed to list consumers: {e}");
                    continue;
                }
            };

            for consumer_name in &consumers {
                let now_ms = now_ms();
                let expired = match store.fetch_expired(consumer_name, now_ms, batch_size) {
                    Ok(msgs) => msgs,
                    Err(e) => {
                        warn!(
                            consumer = consumer_name,
                            "redelivery: fetch_expired failed: {e}"
                        );
                        continue;
                    }
                };

                if expired.is_empty() {
                    continue;
                }

                let consumer = engine.durable_consumers().get(consumer_name);

                for stored in expired {
                    if stored.attempt > max_attempts {
                        // Dead-letter: publish on _dead_letter.{subject} and mark as dead-lettered
                        let original_subject =
                            Subject::from_bytes(&stored.envelope.subject).unwrap_or_default();
                        let dead_subject = Subject::new()
                            .str("_dead_letter")
                            .segment(hermes_core::Segment::s(original_subject.to_string()));
                        debug!(
                            message_id = stored.envelope.id,
                            original_subject = %original_subject,
                            dead_subject = %dead_subject,
                            attempt = stored.attempt,
                            "dead-lettering message"
                        );

                        let mut dead_envelope = stored.envelope.clone();
                        dead_envelope.subject = dead_subject.to_bytes();
                        engine.publish(&dead_envelope);

                        if let Err(e) = store.nack(&stored.envelope.id, consumer_name, false) {
                            warn!(
                                message_id = stored.envelope.id,
                                "failed to dead-letter: {e}"
                            );
                        }
                        continue;
                    }

                    // If consumer is not connected, requeue the message so it
                    // can be picked up on reconnect via fetch_pending.
                    let Some(ref consumer) = consumer else {
                        if let Err(e) = store.nack(&stored.envelope.id, consumer_name, true) {
                            warn!(
                                message_id = stored.envelope.id,
                                "redelivery: requeue failed: {e}"
                            );
                        } else {
                            debug!(
                                consumer = consumer_name,
                                message_id = stored.envelope.id,
                                "redelivery: consumer offline, requeued to pending"
                            );
                        }
                        continue;
                    };

                    // Re-deliver with new deadline.
                    let deadline = now_ms + u64::from(consumer.ack_timeout_secs) * 1000;
                    if let Err(e) =
                        store.mark_delivered(&stored.envelope.id, consumer_name, deadline)
                    {
                        warn!(
                            message_id = stored.envelope.id,
                            "redelivery: mark_delivered failed: {e}"
                        );
                        continue;
                    }

                    let msg = DurableServerMessage {
                        msg: Some(hermes_proto::durable_server_message::Msg::Redelivery(
                            Redelivery {
                                envelope: Some(stored.envelope.clone()),
                                attempt: stored.attempt,
                            },
                        )),
                    };

                    if consumer.sender.try_send(msg).is_err() {
                        warn!(
                            consumer = consumer_name,
                            message_id = stored.envelope.id,
                            "redelivery: consumer channel full or closed"
                        );
                    } else {
                        debug!(
                            consumer = consumer_name,
                            message_id = stored.envelope.id,
                            attempt = stored.attempt,
                            "redelivered"
                        );
                    }
                }
            }
        }
        debug!("redelivery loop stopped");
    })
}

/// Spawn the GC loop that cleans up old acked messages.
/// The loop stops when `cancel` is triggered.
pub fn spawn_gc_loop(
    store: Arc<dyn MessageStore>,
    retention_secs: u64,
    gc_interval_secs: u64,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(gc_interval_secs));
        interval.tick().await;

        loop {
            select! {
                _ = interval.tick() => {}
                () = cancel.cancelled() => break,
            }

            let threshold = now_ms().saturating_sub(retention_secs * 1000);
            match store.gc_acked(threshold) {
                Ok(0) => {}
                Ok(n) => debug!(removed = n, "gc completed"),
                Err(e) => warn!("gc failed: {e}"),
            }
        }
        debug!("gc loop stopped");
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
