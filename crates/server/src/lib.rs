pub mod broker;
pub mod config;
pub mod grpc;
pub mod redelivery;
pub mod subscription;

use std::sync::Arc;

use scylla_broker_proto::broker_server::BrokerServer;
use scylla_broker_store::RedbMessageStore;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::info;

/// Run the broker server on the given listener.
/// Useful for integration tests that need a server on a random port.
pub async fn run(listener: TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    run_with_config(listener, config::ServerConfig::default()).await
}

/// Run the broker server with a specific config.
pub async fn run_with_config(
    listener: TcpListener,
    config: config::ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let store: Option<Arc<dyn scylla_broker_store::MessageStore>> =
        if let Some(ref path) = config.store_path {
            let store = RedbMessageStore::open(path)?;
            info!(?path, "durable store opened");
            Some(Arc::new(store))
        } else {
            None
        };

    let engine = if let Some(ref store) = store {
        Arc::new(broker::BrokerEngine::with_store(
            config.subscriber_channel_capacity,
            store.clone(),
        ))
    } else {
        Arc::new(broker::BrokerEngine::new(
            config.subscriber_channel_capacity,
        ))
    };

    // Spawn redelivery + GC loops if store is enabled.
    if let Some(ref store) = store {
        redelivery::spawn_redelivery_loop(
            engine.clone(),
            config.redelivery_interval_secs,
            config.max_delivery_attempts,
            config.redelivery_batch_size,
        );
        redelivery::spawn_gc_loop(
            store.clone(),
            config.retention_secs,
            config.gc_interval_secs,
        );
    }

    let service = grpc::BrokerService::new(engine, config);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(scylla_broker_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .add_service(reflection)
        .add_service(BrokerServer::new(service))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}
