mod grpc;

use hermes_broker::router::{Router, RouterConfig};
use hermes_proto::broker_server::BrokerServer;
use tonic::transport::Server;
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

    let addr = "[::1]:50051".parse()?;

    let config = RouterConfig::default();
    info!(
        broadcast_capacity = config.broadcast_capacity,
        queue_channel_capacity = config.queue_channel_capacity,
        "router configuration loaded"
    );

    let (router, router_tx) = Router::new(config, 8192);
    tokio::spawn(router.run());

    let service = grpc::BrokerService::new(router_tx);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(hermes_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    info!(%addr, "hermes-broker listening");

    Server::builder()
        .add_service(reflection)
        .add_service(BrokerServer::new(service))
        .serve(addr)
        .await?;

    info!("hermes-broker shutting down");

    Ok(())
}
