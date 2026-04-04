use hermes_proto::broker_client::BrokerClient;
use hermes_proto::{Sub, SubscribeRequest, SubscribeResponse};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, info};

/// A subscriber that receives messages from the broker over a gRPC stream.
/// Drop the subscriber to unsubscribe (closing the stream triggers cleanup).
pub struct Subscriber {
    /// Send sub commands to the gRPC stream.
    cmd_tx: mpsc::Sender<SubscribeRequest>,
    /// Receive deliveries from the broker.
    delivery_rx: mpsc::Receiver<SubscribeResponse>,
}

impl Subscriber {
    /// Create a new subscriber connected to the broker.
    pub async fn new(channel: Channel) -> Result<Self, tonic::Status> {
        let mut client = BrokerClient::new(channel);
        let (cmd_tx, cmd_rx) = mpsc::channel::<SubscribeRequest>(64);
        let (delivery_tx, delivery_rx) = mpsc::channel::<SubscribeResponse>(256);

        let response = client.subscribe(ReceiverStream::new(cmd_rx)).await?;
        let mut resp_stream = response.into_inner();

        // Forward deliveries from gRPC stream to user.
        tokio::spawn(async move {
            while let Ok(Some(msg)) = resp_stream.message().await {
                if delivery_tx.send(msg).await.is_err() {
                    debug!("subscriber delivery channel closed");
                    break;
                }
            }
            debug!("subscriber stream ended");
        });

        info!("subscriber created");
        Ok(Self {
            cmd_tx,
            delivery_rx,
        })
    }

    /// Subscribe to a subject. Optional queue_group for load-balanced delivery.
    pub async fn subscribe(
        &self,
        subject: impl Into<String>,
        queue_group: Option<String>,
    ) -> Result<(), SubscribeError> {
        let subject = subject.into();
        info!(
            subject = %subject,
            queue_group = queue_group.as_deref().unwrap_or("(none)"),
            "subscribing to subject"
        );
        let req = SubscribeRequest {
            sub: Some(Sub {
                subject,
                queue_group: queue_group.unwrap_or_default(),
            }),
        };
        self.cmd_tx
            .send(req)
            .await
            .map_err(|_| SubscribeError::Disconnected)?;
        Ok(())
    }

    /// Receive the next delivery. Returns None if the stream is closed.
    pub async fn recv(&mut self) -> Option<SubscribeResponse> {
        self.delivery_rx.recv().await
    }
}

/// Errors returned by [`Subscriber::subscribe`].
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    /// The connection to the broker has been lost.
    #[error("disconnected from broker")]
    Disconnected,
}
