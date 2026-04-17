//! Graceful shutdown signal for the gRPC server.

use tracing::{error, info};

/// Resolves when the process receives Ctrl+C. Fed into
/// [`tonic::transport::Server::serve_with_shutdown`] so in-flight RPCs
/// get to finish before the binary exits.
pub async fn signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("received shutdown signal (Ctrl+C)"),
        Err(e) => error!(error = %e, "failed to install Ctrl+C handler"),
    }
}
