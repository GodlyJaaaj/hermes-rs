use futures::{Stream, StreamExt};
use hermes_core::{Event, EventGroup, Subject};
use hermes_proto::broker_client::BrokerClient;
use hermes_proto::{EventEnvelope, SubscribeRequest};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, info, trace};
use uuid::Uuid;

use crate::durable_subscriber::{self, DurableSubscriber};
use crate::error::ClientError;
use crate::publisher::{BatchPublisher, make_envelope};
use crate::subscriber;

/// Typed client for the hermes.
#[derive(Clone)]
pub struct HermesClient {
    inner: BrokerClient<Channel>,
    uri: String,
}

impl HermesClient {
    /// Connect to a hermes server.
    pub async fn connect(addr: impl Into<String>) -> Result<Self, ClientError> {
        let uri = addr.into();
        let inner = BrokerClient::connect(uri.clone()).await?;
        info!(uri = %uri, "connected to broker");
        Ok(Self { inner, uri })
    }

    /// Returns the URI this client is connected to.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    // --- Fire-and-forget ---

    /// Publish a single typed event (fire-and-forget).
    pub async fn publish<E: Event>(&self, event: &E) -> Result<(), ClientError> {
        let envelope = make_envelope(event)?;
        trace!(subject = %envelope.subject, id = %envelope.id, "publishing event");
        let stream = tokio_stream::once(envelope);
        let mut client = self.inner.clone();
        let _ack = client.publish(stream).await?;
        Ok(())
    }

    /// Subscribe to a typed Event (fire-and-forget).
    ///
    /// - `queue_groups` empty → fanout (every subscriber receives every message).
    /// - `queue_groups` non-empty → the subscriber joins each listed group.
    pub async fn subscribe<E: Event>(
        &self,
        queue_groups: &[&str],
    ) -> Result<impl Stream<Item = Result<E, ClientError>> + use<E>, ClientError> {
        debug!("subscribing to event stream");
        let client = self.inner.clone();
        subscriber::subscribe_event(client, queue_groups).await
    }

    /// Subscribe to an EventGroup (fire-and-forget).
    pub async fn subscribe_group<G: EventGroup>(
        &self,
        queue_groups: &[&str],
    ) -> Result<impl Stream<Item = Result<G, ClientError>> + use<G>, ClientError> {
        let client = self.inner.clone();
        subscriber::subscribe_group(client, queue_groups).await
    }

    /// Create a BatchPublisher for high-throughput publishing.
    pub fn batch_publisher(&self) -> BatchPublisher {
        let (tx, rx) = mpsc::channel(8192);
        let mut client = self.inner.clone();

        let handle = tokio::spawn(async move {
            let stream = ReceiverStream::new(rx);
            let response = client.publish(stream).await.map_err(ClientError::Rpc)?;
            Ok(response.into_inner())
        });

        debug!("batch publisher created");
        BatchPublisher::new(tx, handle)
    }

    // --- Durable ---

    /// Publish a single typed event with durability (persisted before ack).
    pub async fn publish_durable<E: Event>(&self, event: &E) -> Result<(), ClientError> {
        let envelope = make_envelope(event)?;
        trace!(subject = %envelope.subject, id = %envelope.id, "publishing durable event");
        let stream = tokio_stream::once(envelope);
        let mut client = self.inner.clone();
        let _ack = client.publish_durable(stream).await?;
        Ok(())
    }

    /// Subscribe to a typed Event with durable delivery (at-least-once).
    pub async fn subscribe_durable<E: Event>(
        &self,
        consumer_name: &str,
        queue_groups: &[&str],
        max_in_flight: u32,
        ack_timeout_secs: u32,
    ) -> Result<DurableSubscriber<E>, ClientError> {
        let client = self.inner.clone();
        let subjects = E::subjects();

        // For now, subscribe to the first subject only.
        // Multi-subject durable subscriptions can be added later.
        let subject = subjects.first().ok_or_else(|| ClientError::ChannelClosed)?;

        debug!(consumer_name, subject = %subject.to_json(), max_in_flight, ack_timeout_secs, "subscribing durable");

        durable_subscriber::subscribe_durable(
            client,
            consumer_name,
            subject,
            queue_groups,
            max_in_flight,
            ack_timeout_secs,
        )
        .await
    }

    // --- Raw (untyped) API ---

    /// Publish a raw envelope with an explicit subject and payload.
    pub async fn publish_raw(
        &self,
        subject: &Subject,
        payload: Vec<u8>,
    ) -> Result<(), ClientError> {
        let envelope = EventEnvelope {
            id: Uuid::now_v7().to_string(),
            subject: subject.to_json(),
            payload,
            headers: Default::default(),
            timestamp_nanos: now_nanos(),
        };
        trace!(subject = %envelope.subject, id = %envelope.id, "publishing raw event");
        let stream = tokio_stream::once(envelope);
        let mut client = self.inner.clone();
        let _ack = client.publish(stream).await?;
        Ok(())
    }

    /// Subscribe to a raw subject (JSON-encoded subject array, e.g. `["job","*","logs"]`).
    /// Returns raw `EventEnvelope`s — useful for debugging, REPL, or dynamic subjects.
    pub async fn subscribe_raw(
        &self,
        subject_json: &str,
        queue_groups: &[&str],
    ) -> Result<impl Stream<Item = Result<EventEnvelope, ClientError>> + use<>, ClientError> {
        let request = SubscribeRequest {
            subject: subject_json.to_string(),
            queue_groups: queue_groups.iter().map(|s| s.to_string()).collect(),
        };
        let mut client = self.inner.clone();
        let response = client.subscribe(request).await?;
        let inner = response.into_inner();
        Ok(inner.map(|r| r.map_err(ClientError::Rpc)))
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
