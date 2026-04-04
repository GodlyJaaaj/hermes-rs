use bytes::Bytes;
use hermes_proto::PublishRequest;
use hermes_proto::broker_client::BrokerClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

/// A fire-and-forget publisher. Messages are streamed to the broker with no per-message ack.
pub struct Publisher {
    tx: mpsc::Sender<PublishRequest>,
}

impl Publisher {
    /// Create a new publisher connected to the broker.
    pub async fn new(channel: Channel) -> Result<Self, tonic::Status> {
        let mut client = BrokerClient::new(channel);
        let (tx, rx) = mpsc::channel::<PublishRequest>(256);

        // Open the client-streaming RPC. The response (PublishAck) comes when we close.
        tokio::spawn(async move {
            let _ = client.publish(ReceiverStream::new(rx)).await;
        });

        Ok(Self { tx })
    }

    /// Publish a message. Returns immediately after queuing (fire-and-forget).
    pub async fn publish(
        &self,
        subject: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<(), PublishError> {
        let req = PublishRequest {
            subject: subject.into(),
            payload: payload.into().to_vec(),
            reply_to: String::new(),
        };

        self.tx
            .send(req)
            .await
            .map_err(|_| PublishError::Disconnected)?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("disconnected from broker")]
    Disconnected,
}
