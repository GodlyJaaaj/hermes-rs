use hermes_core::{Event, Subject};
use hermes_proto::{
    Ack, DurableClientMessage, DurableSubscribeRequest, Nack, broker_client::BrokerClient,
    durable_client_message::Msg as ClientMsg, durable_server_message::Msg as ServerMsg,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::error::ClientError;

/// A message received from a durable subscription.
/// Must be explicitly acked or nacked.
pub struct DurableMessage<E> {
    pub event: E,
    pub message_id: String,
    pub attempt: u32,
    ack_tx: mpsc::Sender<DurableClientMessage>,
}

impl<E> DurableMessage<E> {
    /// Acknowledge this message. It won't be redelivered.
    pub async fn ack(self) -> Result<(), ClientError> {
        let msg = DurableClientMessage {
            msg: Some(ClientMsg::Ack(Ack {
                message_id: self.message_id,
            })),
        };
        self.ack_tx
            .send(msg)
            .await
            .map_err(|_| ClientError::ChannelClosed)
    }

    /// Negative acknowledge. If `requeue` is true, the message will be
    /// redelivered immediately. If false, it goes to the dead-letter subject.
    pub async fn nack(self, requeue: bool) -> Result<(), ClientError> {
        let msg = DurableClientMessage {
            msg: Some(ClientMsg::Nack(Nack {
                message_id: self.message_id,
                requeue,
            })),
        };
        self.ack_tx
            .send(msg)
            .await
            .map_err(|_| ClientError::ChannelClosed)
    }
}

/// A durable subscriber that receives messages and sends ack/nack.
pub struct DurableSubscriber<E> {
    rx: mpsc::Receiver<Result<DurableMessage<E>, ClientError>>,
}

impl<E> DurableSubscriber<E> {
    /// Receive the next message. Returns None if the stream is closed.
    pub async fn next(&mut self) -> Option<Result<DurableMessage<E>, ClientError>> {
        self.rx.recv().await
    }
}

/// Create a durable subscriber for a typed Event.
pub(crate) async fn subscribe_durable<E: Event>(
    mut client: BrokerClient<Channel>,
    consumer_name: &str,
    subject: &Subject,
    queue_groups: &[&str],
    max_in_flight: u32,
    ack_timeout_secs: u32,
) -> Result<DurableSubscriber<E>, ClientError> {
    let capacity = max_in_flight as usize;

    // Single channel for all outgoing messages (subscribe + ack/nack).
    let (outgoing_tx, outgoing_rx) = mpsc::channel::<DurableClientMessage>(capacity);

    // Push subscribe request as the first message.
    outgoing_tx
        .send(DurableClientMessage {
            msg: Some(ClientMsg::Subscribe(DurableSubscribeRequest {
                subject: subject.to_json(),
                consumer_name: consumer_name.to_string(),
                queue_groups: queue_groups.iter().map(|s| s.to_string()).collect(),
                max_in_flight,
                ack_timeout_seconds: ack_timeout_secs,
            })),
        })
        .await
        .map_err(|_| ClientError::ChannelClosed)?;

    // Open the bidi stream.
    let response = client
        .subscribe_durable(ReceiverStream::new(outgoing_rx))
        .await?;
    let mut server_stream = response.into_inner();

    // Channel for decoded messages to the caller.
    let (msg_tx, msg_rx) = mpsc::channel(capacity);

    // The outgoing_tx is cloned into each DurableMessage for ack/nack.
    let ack_sender = outgoing_tx;

    tokio::spawn(async move {
        while let Ok(Some(server_msg)) = server_stream.message().await {
            let (envelope, attempt) = match server_msg.msg {
                Some(ServerMsg::Envelope(env)) => (env, 1),
                Some(ServerMsg::Redelivery(redel)) => match redel.envelope {
                    Some(env) => (env, redel.attempt),
                    None => continue,
                },
                None => continue,
            };

            let message_id = envelope.id.clone();
            let decoded = match hermes_core::decode::<E>(&envelope.payload) {
                Ok(e) => e,
                Err(e) => {
                    let _ = msg_tx.send(Err(ClientError::Decode(e))).await;
                    continue;
                }
            };

            let durable_msg = DurableMessage {
                event: decoded,
                message_id,
                attempt,
                ack_tx: ack_sender.clone(),
            };

            if msg_tx.send(Ok(durable_msg)).await.is_err() {
                break;
            }
        }
    });

    Ok(DurableSubscriber { rx: msg_rx })
}
