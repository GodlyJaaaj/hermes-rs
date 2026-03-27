//! `hermes-server` is the server-side crate for the Hermes gRPC event broker.
//!
//! It provides:
//! - a broker engine with fanout and queue-group delivery
//! - a gRPC service implementation (via `tonic`)
//! - optional durable delivery with redelivery + garbage-collection loops
//!
//! # Quick start
//!
//! Run a server from a bound `TcpListener`:
//!
//! ```no_run
//! use tokio::net::TcpListener;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let listener = TcpListener::bind("127.0.0.1:4222").await?;
//!     hermes_server::run(listener).await?;
//!     Ok(())
//! }
//! ```
//!
//! Run with explicit configuration:
//!
//! ```no_run
//! use tokio::net::TcpListener;
//! use hermes_server::config::ServerConfig;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let listener = TcpListener::bind("127.0.0.1:4222").await?;
//!     let mut cfg = ServerConfig::default();
//!     cfg.subscriber_channel_capacity = 16_384;
//!     cfg.store_path = Some("hermes.redb".into());
//!     hermes_server::run_with_config(listener, cfg).await?;
//!     Ok(())
//! }
//! ```
//!
//! # Configuration
//!
//! The server can be configured programmatically with [`config::ServerConfig`] and can
//! also be initialized from environment variables through `ServerConfig::from_env()`.
//!
//! Useful env vars include:
//! - `HERMES_LISTEN_ADDR`
//! - `HERMES_CHANNEL_CAPACITY`
//! - `HERMES_GRPC_OUTPUT_BUFFER`
//! - `HERMES_STORE_PATH`
//! - `HERMES_REDELIVERY_INTERVAL`
//! - `HERMES_MAX_DELIVERY_ATTEMPTS`
//! - `HERMES_RETENTION_SECS`
//! - `HERMES_ACK_TIMEOUT`
//! - `HERMES_MAX_IN_FLIGHT`
//! - `HERMES_GC_INTERVAL`
//! - `HERMES_REDELIVERY_BATCH_SIZE`
//!
//! # Notes
//!
//! - If `store_path` is `None`, the server runs in fire-and-forget mode (no durable
//!   redelivery persistence).
//! - If `store_path` is set, durable store-backed redelivery and GC loops are spawned.

pub mod broker;
pub mod config;
pub mod grpc;
pub mod redelivery;
pub mod subscription;

use std::future::Future;
use std::sync::Arc;

use hermes_proto::broker_server::BrokerServer;
use hermes_store::RedbMessageStore;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;

/// Run the broker server on the given listener (runs until the process is killed).
/// Useful for integration tests that need a server on a random port.
pub async fn run(listener: TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    run_with_config(listener, config::ServerConfig::default()).await
}

/// Run the broker server with a specific config (runs until the process is killed).
pub async fn run_with_config(
    listener: TcpListener,
    config: config::ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // A future that never resolves — the server runs forever.
    run_with_shutdown(listener, config, std::future::pending::<()>()).await
}

/// Run the broker server with graceful shutdown.
///
/// The server will stop accepting new connections when `shutdown` resolves,
/// and will finish processing in-flight requests before returning.
pub async fn run_with_shutdown(
    listener: TcpListener,
    config: config::ServerConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let store: Option<Arc<dyn hermes_store::MessageStore>> =
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

    // Token to cancel background loops on shutdown.
    let cancel = CancellationToken::new();

    // Spawn redelivery + GC loops if store is enabled.
    if let Some(ref store) = store {
        redelivery::spawn_redelivery_loop(
            engine.clone(),
            config.redelivery_interval_secs,
            config.max_delivery_attempts,
            config.redelivery_batch_size,
            cancel.clone(),
        );
        redelivery::spawn_gc_loop(
            store.clone(),
            config.retention_secs,
            config.gc_interval_secs,
            cancel.clone(),
        );
    }

    let service = grpc::BrokerService::new(engine, config);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(hermes_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .add_service(reflection)
        .add_service(BrokerServer::new(service))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;

    // Stop background loops and let them release the store.
    cancel.cancel();

    Ok(())
}
