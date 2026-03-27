use hermes_core::{Event, Subject};
use hermes_proto::{EventEnvelope, PublishAck};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::ClientError;

/// High-throughput publisher that keeps a single gRPC client-stream open.
///
/// All messages sent via [`send`](BatchPublisher::send) or
/// [`send_raw`](BatchPublisher::send_raw) flow through the same HTTP/2 stream,
/// avoiding the per-message round-trip overhead of individual publish calls.
pub struct BatchPublisher {
    sender: mpsc::Sender<EventEnvelope>,
    handle: JoinHandle<Result<PublishAck, ClientError>>,
}

impl BatchPublisher {
    pub(crate) fn new(
        sender: mpsc::Sender<EventEnvelope>,
        handle: JoinHandle<Result<PublishAck, ClientError>>,
    ) -> Self {
        Self { sender, handle }
    }

    /// Send a typed event through the batch stream.
    pub async fn send<E: Event>(&self, event: &E) -> Result<(), ClientError> {
        let envelope = make_envelope(event)?;
        self.sender
            .send(envelope)
            .await
            .map_err(|_| ClientError::ChannelClosed)
    }

    /// Send a raw envelope (explicit subject + payload) through the batch stream.
    pub async fn send_raw(&self, subject: &Subject, payload: Vec<u8>) -> Result<(), ClientError> {
        let envelope = EventEnvelope {
            id: Uuid::now_v7().to_string(),
            subject: subject.to_json(),
            payload,
            headers: Default::default(),
            timestamp_nanos: now_nanos(),
        };
        self.sender
            .send(envelope)
            .await
            .map_err(|_| ClientError::ChannelClosed)
    }

    /// Close the stream and wait for the server ack.
    pub async fn flush(self) -> Result<PublishAck, ClientError> {
        drop(self.sender);
        self.handle.await.unwrap_or(Err(ClientError::ChannelClosed))
    }
}

fn now_nanos() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(i64::MAX)
}

pub(crate) fn make_envelope<E: Event>(event: &E) -> Result<EventEnvelope, ClientError> {
    let payload = hermes_core::encode(event)?;
    Ok(EventEnvelope {
        id: Uuid::now_v7().to_string(),
        subject: event.subject().to_json(),
        payload,
        headers: Default::default(),
        timestamp_nanos: i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX),
    })
}
