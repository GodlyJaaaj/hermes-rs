use futures::stream::{self, Stream, StreamExt};
use hermes_core::{Event, EventGroup, Subject};
use hermes_proto::{SubscribeRequest, broker_client::BrokerClient};
use tonic::transport::Channel;
use tracing::debug;

use crate::error::ClientError;

/// Subscribe to a single Event type (struct or enum).
/// For enums with multiple variants, opens N subscriptions and merges them.
pub(crate) async fn subscribe_event<E: Event>(
    client: BrokerClient<Channel>,
    queue_groups: &[&str],
) -> Result<impl Stream<Item = Result<E, ClientError>> + use<E>, ClientError> {
    let subjects = E::subjects();
    let qg: Vec<String> = queue_groups.iter().map(|s| s.to_string()).collect();

    debug!(
        subject_count = subjects.len(),
        "subscribing to event subjects"
    );

    let mut streams = Vec::with_capacity(subjects.len());

    for subject in subjects {
        let request = SubscribeRequest {
            subject: subject.to_bytes(),
            queue_groups: qg.clone(),
        };

        let mut c = client.clone();
        let response = c.subscribe(request).await?;
        let inner = response.into_inner();

        debug!(subject = %subject, "subscribed to subject stream");

        let mapped = inner.map(|result| {
            let envelope = result.map_err(ClientError::Rpc)?;
            hermes_core::decode::<E>(&envelope.payload).map_err(ClientError::Decode)
        });

        streams.push(mapped);
    }

    Ok(stream::select_all(streams))
}

/// Subscribe to an EventGroup (multiple struct types merged via event_group!).
pub(crate) async fn subscribe_group<G: EventGroup>(
    client: BrokerClient<Channel>,
    queue_groups: &[&str],
) -> Result<impl Stream<Item = Result<G, ClientError>> + use<G>, ClientError> {
    let subjects = G::subjects();
    let qg: Vec<String> = queue_groups.iter().map(|s| s.to_string()).collect();

    debug!(subject_count = subjects.len(), "subscribing to event group");

    let mut streams = Vec::with_capacity(subjects.len());

    for subject in subjects {
        let request = SubscribeRequest {
            subject: subject.to_bytes(),
            queue_groups: qg.clone(),
        };

        let mut c = client.clone();
        let response = c.subscribe(request).await?;
        let inner = response.into_inner();

        debug!(subject = %subject, "subscribed to subject stream");

        let mapped = inner.map(move |result| {
            let envelope = result.map_err(ClientError::Rpc)?;
            let subject = Subject::from_bytes(&envelope.subject)
                .map_err(ClientError::Decode)?;
            G::decode_event(&subject, &envelope.payload).map_err(ClientError::Decode)
        });

        streams.push(mapped);
    }

    Ok(stream::select_all(streams))
}
