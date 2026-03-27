use std::sync::Arc;

use scylla_broker_proto::broker_server::BrokerServer;
use scylla_broker_server::broker::BrokerEngine;
use scylla_broker_server::config::ServerConfig;
use scylla_broker_server::grpc::BrokerService;
use scylla_broker_store::{MessageStore, RedbMessageStore};
use tonic::transport::Server;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = ServerConfig::from_env();

    let store: Option<Arc<dyn MessageStore>> = if let Some(ref path) = config.store_path {
        let store = RedbMessageStore::open(path)?;
        info!(?path, "durable store opened");
        Some(Arc::new(store))
    } else {
        info!("running in fire-and-forget mode (no SCYLLA_STORE_PATH)");
        None
    };

    let engine = if let Some(ref store) = store {
        Arc::new(BrokerEngine::with_store(
            config.subscriber_channel_capacity,
            store.clone(),
        ))
    } else {
        Arc::new(BrokerEngine::new(config.subscriber_channel_capacity))
    };

    // Spawn redelivery + GC loops if durable mode is enabled.
    if let Some(ref store) = store {
        scylla_broker_server::redelivery::spawn_redelivery_loop(
            engine.clone(),
            config.redelivery_interval_secs,
            config.max_delivery_attempts,
            config.redelivery_batch_size,
        );
        scylla_broker_server::redelivery::spawn_gc_loop(
            store.clone(),
            config.retention_secs,
            config.gc_interval_secs,
        );
        info!("redelivery and GC loops started");
    }

    let listen_addr = config.listen_addr;
    let service = BrokerService::new(engine, config);

    info!("scylla-broker listening on {listen_addr}");

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(scylla_broker_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .add_service(reflection)
        .add_service(BrokerServer::new(service))
        .serve_with_shutdown(listen_addr, shutdown_signal())
        .await?;

    info!("scylla-broker shut down");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("shutdown signal received");
}
