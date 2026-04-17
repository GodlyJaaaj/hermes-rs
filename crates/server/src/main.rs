mod cli;
mod delivery_rx;
mod grpc;
mod shutdown;
mod tls;

use clap::Parser;
use hermes_broker::router::{Router, RouterConfig};
use hermes_proto::broker_server::BrokerServer;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use cli::Args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    init_logging();

    let addr = args.addr.parse()?;
    let config = RouterConfig::default();
    info!(
        addr = %args.addr,
        router_capacity = args.router_capacity,
        broadcast_capacity = config.broadcast_capacity,
        queue_channel_capacity = config.queue_channel_capacity,
        "router configuration loaded"
    );

    let (router, router_tx) = Router::new(config, args.router_capacity);
    tokio::spawn(router.run());

    let service = grpc::BrokerService::new(router_tx);
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(hermes_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    info!(%addr, "hermes-broker listening");

    let mut builder = Server::builder();
    if let Some(tls_config) = tls::load_from_env().await? {
        builder = builder.tls_config(tls_config)?;
    }

    builder
        .add_service(reflection)
        .add_service(BrokerServer::new(service))
        .serve_with_shutdown(addr, shutdown::signal())
        .await?;

    info!("hermes-broker shutting down");

    Ok(())
}

/// Set up the tracing subscriber. Respects `RUST_LOG` for filtering and
/// `HERMES_LOG_FORMAT=json` to switch to structured JSON output.
fn init_logging() {
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
}
