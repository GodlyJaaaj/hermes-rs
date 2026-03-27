use scylla_broker_core::Event;
use scylla_broker_proto::{EventEnvelope, PublishAck};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::ClientError;

/// High-throughput publisher that keeps a gRPC client-stream open.
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

    /// Close the stream and wait for the server ack.
    pub async fn flush(self) -> Result<PublishAck, ClientError> {
        drop(self.sender);
        self.handle.await.unwrap_or(Err(ClientError::ChannelClosed))
    }
}

pub(crate) fn make_envelope<E: Event>(event: &E) -> Result<EventEnvelope, ClientError> {
    let payload = scylla_broker_core::encode(event)?;
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
