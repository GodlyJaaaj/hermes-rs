use std::sync::Arc;

use hermes_core::Subject;
use hermes_proto::{
    DurableClientMessage, DurableServerMessage, EventEnvelope, PublishAck, PublishDurableAck,
    SubscribeRequest, broker_server::Broker, durable_client_message::Msg as ClientMsg,
};
use tokio::select;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::broker::BrokerEngine;
use crate::config::ServerConfig;
use crate::subscription::SubscriptionReceiver;

pub struct BrokerService {
    engine: Arc<BrokerEngine>,
    config: ServerConfig,
}

impl BrokerService {
    pub fn new(engine: Arc<BrokerEngine>, config: ServerConfig) -> Self {
        Self { engine, config }
    }
}

#[tonic::async_trait]
impl Broker for BrokerService {
    // --- Fire-and-forget ---

    async fn publish(
        &self,
        request: Request<Streaming<EventEnvelope>>,
    ) -> Result<Response<PublishAck>, Status> {
        let mut stream = request.into_inner();
        let mut accepted: u64 = 0;

        while let Some(envelope) = stream.message().await? {
            if envelope.subject.is_empty() {
                return Err(Status::invalid_argument("subject must not be empty"));
            }
            if envelope.id.is_empty() {
                return Err(Status::invalid_argument("id must not be empty"));
            }

            let _delivered = self.engine.publish(&envelope);
            accepted = accepted
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("too many messages in single stream"))?;
        }

        debug!(accepted, "publish stream completed");
        Ok(Response::new(PublishAck { accepted }))
    }

    type SubscribeStream = ReceiverStream<Result<EventEnvelope, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();

        if req.subject.is_empty() {
            return Err(Status::invalid_argument("subject must not be empty"));
        }

        let subject = Subject::from_bytes(&req.subject)
            .map_err(|e| Status::invalid_argument(format!("invalid subject: {e}")))?;

        let (id, receiver) = self.engine.subscribe(subject.clone(), req.queue_groups);

        let (tx_out, rx_out) = tokio::sync::mpsc::channel(self.config.grpc_output_buffer);
        let engine = self.engine.clone();

        tokio::spawn(async move {
            match receiver {
                SubscriptionReceiver::Fanout(mut rx) => loop {
                    select! {
                        result = rx.recv() => {
                            match result {
                                Ok(arc_env) => {
                                    let env = Arc::unwrap_or_clone(arc_env);
                                    if tx_out.send(Ok(env)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                    warn!(subject = %subject, "subscriber lagged, missed {n} messages");
                                    continue;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            }
                        }
                        _ = tx_out.closed() => break,
                    }
                },
                SubscriptionReceiver::QueueGroup(mut rx) => loop {
                    select! {
                        msg = rx.recv() => {
                            match msg {
                                Some(arc_env) => {
                                    let env = Arc::unwrap_or_clone(arc_env);
                                    if tx_out.send(Ok(env)).await.is_err() {
                                        break;
                                    }
                                }
                                None => break,
                            }
                        }
                        _ = tx_out.closed() => break,
                    }
                },
            }
            engine.unsubscribe(&subject, id);
            debug!(subject = %subject, %id, "subscriber stream ended");
        });

        Ok(Response::new(ReceiverStream::new(rx_out)))
    }

    // --- Durable ---

    async fn publish_durable(
        &self,
        request: Request<Streaming<EventEnvelope>>,
    ) -> Result<Response<PublishDurableAck>, Status> {
        if self.engine.store().is_none() {
            return Err(Status::failed_precondition(
                "durable mode not enabled: configure HERMES_STORE_PATH",
            ));
        }

        let mut stream = request.into_inner();
        let mut accepted: u64 = 0;
        let mut persisted: u64 = 0;

        while let Some(envelope) = stream.message().await? {
            if envelope.subject.is_empty() {
                return Err(Status::invalid_argument("subject must not be empty"));
            }
            if envelope.id.is_empty() {
                return Err(Status::invalid_argument("id must not be empty"));
            }

            match self.engine.publish_durable(&envelope) {
                Ok(_delivered) => {
                    persisted = persisted.checked_add(1).ok_or_else(|| {
                        Status::resource_exhausted("too many messages in single stream")
                    })?;
                }
                Err(e) => {
                    warn!(id = envelope.id, "publish_durable failed: {e}");
                    return Err(Status::internal(format!("persist failed: {e}")));
                }
            }

            accepted = accepted
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("too many messages in single stream"))?;
        }

        debug!(accepted, persisted, "publish_durable stream completed");
        Ok(Response::new(PublishDurableAck {
            accepted,
            persisted,
        }))
    }

    type SubscribeDurableStream = ReceiverStream<Result<DurableServerMessage, Status>>;

    async fn subscribe_durable(
        &self,
        request: Request<Streaming<DurableClientMessage>>,
    ) -> Result<Response<Self::SubscribeDurableStream>, Status> {
        if self.engine.store().is_none() {
            return Err(Status::failed_precondition(
                "durable mode not enabled: configure HERMES_STORE_PATH",
            ));
        }

        let mut client_stream = request.into_inner();

        // First message must be a DurableSubscribeRequest.
        let first = client_stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("expected DurableSubscribeRequest"))?;

        let sub_req = match first.msg {
            Some(ClientMsg::Subscribe(req)) => req,
            _ => {
                return Err(Status::invalid_argument(
                    "first message must be DurableSubscribeRequest",
                ));
            }
        };

        if sub_req.subject.is_empty() {
            return Err(Status::invalid_argument("subject must not be empty"));
        }
        if sub_req.consumer_name.is_empty() {
            return Err(Status::invalid_argument("consumer_name must not be empty"));
        }

        // Validate subject is well-formed
        let _subject = Subject::from_bytes(&sub_req.subject)
            .map_err(|e| Status::invalid_argument(format!("invalid subject: {e}")))?;

        let max_in_flight = if sub_req.max_in_flight == 0 {
            self.config.default_max_in_flight
        } else {
            sub_req.max_in_flight
        };

        let ack_timeout = if sub_req.ack_timeout_seconds == 0 {
            self.config.default_ack_timeout_secs
        } else {
            sub_req.ack_timeout_seconds
        };

        let consumer_name = sub_req.consumer_name.clone();
        let (connection_id, rx) = self
            .engine
            .subscribe_durable(
                consumer_name.clone(),
                sub_req.subject.clone(),
                sub_req.queue_groups,
                max_in_flight,
                ack_timeout,
            )
            .map_err(|e| Status::internal(format!("subscribe_durable failed: {e}")))?;

        let (tx_out, rx_out) = tokio::sync::mpsc::channel(max_in_flight as usize);
        let engine = self.engine.clone();
        let consumer_name_for_ack = consumer_name.clone();

        // Shared token to cancel both tasks when either side terminates.
        let cancel = CancellationToken::new();

        // Task: pipe messages from engine to gRPC output stream.
        let tx_out_clone = tx_out.clone();
        let cancel_outbound = cancel.clone();
        tokio::spawn(async move {
            let mut rx = rx;
            loop {
                select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if tx_out_clone.send(Ok(msg)).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    () = cancel_outbound.cancelled() => break,
                }
            }
            cancel_outbound.cancel();
        });

        // Task: read ack/nack from client stream.
        let engine_ack = self.engine.clone();
        let cancel_inbound = cancel.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    msg = client_stream.message() => {
                        match msg {
                            Ok(Some(msg)) => match msg.msg {
                                Some(ClientMsg::Ack(ack)) => {
                                    if let Err(e) =
                                        engine_ack.ack_message(&ack.message_id, &consumer_name_for_ack)
                                    {
                                        warn!(
                                            message_id = ack.message_id,
                                            consumer = consumer_name_for_ack,
                                            "ack failed: {e}"
                                        );
                                    }
                                }
                                Some(ClientMsg::Nack(nack)) => {
                                    if let Err(e) = engine_ack.nack_message(
                                        &nack.message_id,
                                        &consumer_name_for_ack,
                                        nack.requeue,
                                    ) {
                                        warn!(
                                            message_id = nack.message_id,
                                            consumer = consumer_name_for_ack,
                                            "nack failed: {e}"
                                        );
                                    }
                                }
                                Some(ClientMsg::Subscribe(_)) => {
                                    warn!(
                                        consumer = consumer_name_for_ack,
                                        "unexpected subscribe message after initial"
                                    );
                                }
                                None => {}
                            },
                            _ => break,
                        }
                    }
                    () = cancel_inbound.cancelled() => break,
                }
            }

            // Client disconnected — messages in-flight stay "delivered" and will be redelivered.
            cancel_inbound.cancel();
            engine.unsubscribe_durable(&consumer_name, connection_id);
            debug!(
                consumer = consumer_name,
                connection_id, "durable subscriber disconnected"
            );
        });

        Ok(Response::new(ReceiverStream::new(rx_out)))
    }
}
