use futures::stream::{self, Stream, StreamExt};
use scylla_broker_core::{Event, EventGroup, Subject};
use scylla_broker_proto::{SubscribeRequest, broker_client::BrokerClient};
use tonic::transport::Channel;

use crate::error::ClientError;

/// Subscribe to a single Event type (struct or enum).
/// For enums with multiple variants, opens N subscriptions and merges them.
pub(crate) async fn subscribe_event<E: Event>(
    client: BrokerClient<Channel>,
    queue_groups: &[&str],
) -> Result<impl Stream<Item = Result<E, ClientError>> + use<E>, ClientError> {
    let subjects = E::subjects();
    let qg: Vec<String> = queue_groups.iter().map(|s| s.to_string()).collect();

    let mut streams = Vec::with_capacity(subjects.len());

    for subject in subjects {
        let request = SubscribeRequest {
            subject: subject.to_json(),
            queue_groups: qg.clone(),
        };

        let mut c = client.clone();
        let response = c.subscribe(request).await?;
        let inner = response.into_inner();

        let mapped = inner.map(|result| {
            let envelope = result.map_err(ClientError::Rpc)?;
            scylla_broker_core::decode::<E>(&envelope.payload).map_err(ClientError::Decode)
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

    let mut streams = Vec::with_capacity(subjects.len());

    for subject in subjects {
        let request = SubscribeRequest {
            subject: subject.to_json(),
            queue_groups: qg.clone(),
        };

        let mut c = client.clone();
        let response = c.subscribe(request).await?;
        let inner = response.into_inner();

        let mapped = inner.map(move |result| {
            let envelope = result.map_err(ClientError::Rpc)?;
            let subject = Subject::from_json(&envelope.subject).map_err(|e| {
                ClientError::Decode(scylla_broker_core::DecodeError::InvalidSubject(
                    e.to_string(),
                ))
            })?;
            G::decode_event(&subject, &envelope.payload).map_err(ClientError::Decode)
        });

        streams.push(mapped);
    }

    Ok(stream::select_all(streams))
}
