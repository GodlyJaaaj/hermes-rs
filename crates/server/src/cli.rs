//! Command-line interface for the `hermes-broker-server` binary.
//!
//! Every flag also has a matching `HERMES_*` env var via clap's `env`
//! attribute. Priority order: explicit flag > env var > default.

use clap::Parser;

/// gRPC broker for the Hermes message router.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Bind address for the gRPC listener.
    #[arg(long, env = "HERMES_ADDR", default_value = "[::1]:50051")]
    pub addr: String,

    /// Capacity of the router command mpsc channel
    /// (publish/subscribe backpressure threshold).
    #[arg(long, env = "HERMES_ROUTER_CAPACITY", default_value_t = 8192)]
    pub router_capacity: usize,
}
