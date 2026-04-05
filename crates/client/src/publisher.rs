use bytes::Bytes;
use hermes_proto::PublishRequest;
use hermes_proto::broker_client::BrokerClient;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, info, trace, warn};

/// A fire-and-forget publisher with automatic reconnection.
///
/// Messages are streamed to the broker. If the stream breaks (broker restart,
/// network hiccup), the publisher transparently reconnects and resumes.
pub struct Publisher {
    tx: mpsc::Sender<PublishRequest>,
}

impl Publisher {
    /// Create a new publisher backed by the given gRPC channel.
    ///
    /// Spawns a background task that manages the publish stream and
    /// reconnects automatically on failure.
    pub fn new(channel: Channel) -> Self {
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(256);

        tokio::spawn(async move {
            loop {
                let mut client = BrokerClient::new(channel.clone());
                let (stream_tx, stream_rx) = mpsc::channel::<PublishRequest>(256);

                let handle = tokio::spawn(async move {
                    client.publish(ReceiverStream::new(stream_rx)).await
                });

                loop {
                    match rx.recv().await {
                        Some(msg) => {
                            if stream_tx.send(msg).await.is_err() {
                                warn!("publish stream broken, reconnecting...");
                                break;
                            }
                        }
                        None => {
                            drop(stream_tx);
                            let _ = handle.await;
                            info!("publisher stopped");
                            return;
                        }
                    }
                }

                drop(stream_tx);
                let _ = handle.await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                info!("reconnecting publish stream...");
            }
        });

        debug!("publisher created");
        Self { tx }
    }

    /// Create a no-op publisher that silently drops all messages (for tests).
    pub fn noop() -> Self {
        let (tx, _rx) = mpsc::channel::<PublishRequest>(1);
        Self { tx }
    }

    /// Publish a message. Returns immediately after queuing (fire-and-forget).
    pub async fn publish(
        &self,
        subject: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<(), PublishError> {
        self.publish_with_reply(subject, payload, "").await
    }

    /// Publish a message with a reply-to subject.
    pub async fn publish_with_reply(
        &self,
        subject: impl Into<String>,
        payload: impl Into<Bytes>,
        reply_to: impl Into<String>,
    ) -> Result<(), PublishError> {
        let req = PublishRequest {
            subject: subject.into(),
            payload: payload.into().to_vec(),
            reply_to: reply_to.into(),
        };

        trace!(subject = %req.subject, "queuing publish");

        self.tx
            .send(req)
            .await
            .map_err(|_| PublishError::Disconnected)?;

        Ok(())
    }
}

/// Errors returned by [`Publisher::publish`].
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// The connection to the broker has been lost.
    #[error("disconnected from broker")]
    Disconnected,
}
