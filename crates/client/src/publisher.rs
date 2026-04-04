use bytes::Bytes;
use hermes_proto::PublishRequest;
use hermes_proto::broker_client::BrokerClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, info, trace, warn};

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
            match client.publish(ReceiverStream::new(rx)).await {
                Ok(resp) => {
                    let ack = resp.into_inner();
                    info!(
                        total_published = ack.total_published,
                        "publish stream completed"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "publish stream failed");
                }
            }
        });

        debug!("publisher created");
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

        trace!(subject = %req.subject, "queuing publish");

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
