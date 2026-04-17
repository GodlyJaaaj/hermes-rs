mod grpc;

use hermes_broker::router::{Router, RouterConfig};
use hermes_proto::broker_server::BrokerServer;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber.
    // Respects RUST_LOG env var; defaults to "info" if unset.
    // Set HERMES_LOG_FORMAT=json for JSON output (e.g. in containers).
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = if std::env::var("HERMES_LOG_FORMAT").as_deref() == Ok("json") {
        fmt::layer().json().boxed()
    } else {
        fmt::layer().with_target(true).boxed()
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();

    let addr_str =
        std::env::var("HERMES_ADDR").unwrap_or_else(|_| "[::1]:50051".to_string());
    let addr = addr_str.parse()?;

    let router_capacity: usize = std::env::var("HERMES_ROUTER_CAPACITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8192);

    let config = RouterConfig::default();
    info!(
        addr = %addr_str,
        router_capacity,
        broadcast_capacity = config.broadcast_capacity,
        queue_channel_capacity = config.queue_channel_capacity,
        "router configuration loaded"
    );

    let (router, router_tx) = Router::new(config, router_capacity);
    tokio::spawn(router.run());

    let service = grpc::BrokerService::new(router_tx);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(hermes_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    info!(%addr, "hermes-broker listening");

    let mut builder = Server::builder();

    let tls_cert = std::env::var("HERMES_TLS_CERT").ok();
    let tls_key = std::env::var("HERMES_TLS_KEY").ok();
    let tls_ca = std::env::var("HERMES_TLS_CA").ok();

    if let (Some(cert_path), Some(key_path), Some(ca_path)) = (tls_cert, tls_key, tls_ca) {
        let cert = tokio::fs::read(&cert_path).await?;
        let key = tokio::fs::read(&key_path).await?;
        let ca = tokio::fs::read(&ca_path).await?;

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(cert, key))
            .client_ca_root(Certificate::from_pem(ca));

        builder = builder.tls_config(tls_config)?;
        info!("mTLS enabled");
    } else {
        info!("TLS not configured, serving plaintext");
    }

    builder
        .add_service(reflection)
        .add_service(BrokerServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    info!("hermes-broker shutting down");

    Ok(())
}

async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("received shutdown signal (Ctrl+C)"),
        Err(e) => tracing::error!(error = %e, "failed to install Ctrl+C handler"),
    }
}
