//! mTLS configuration helper for the gRPC server. Reads `HERMES_TLS_CERT`,
//! `HERMES_TLS_KEY`, and `HERMES_TLS_CA` and produces a [`ServerTlsConfig`]
//! when all three are set. Absent env vars means plaintext.

use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tracing::info;

/// Load TLS material from the standard `HERMES_TLS_*` env vars.
///
/// Returns `Ok(Some(config))` if all three paths are set and readable,
/// `Ok(None)` if any is missing (caller falls back to plaintext),
/// or `Err` if a file exists but cannot be read / parsed.
pub async fn load_from_env() -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    let Some(cert_path) = std::env::var("HERMES_TLS_CERT").ok() else {
        info!("TLS not configured, serving plaintext");
        return Ok(None);
    };
    let Some(key_path) = std::env::var("HERMES_TLS_KEY").ok() else {
        info!("TLS not configured, serving plaintext");
        return Ok(None);
    };
    let Some(ca_path) = std::env::var("HERMES_TLS_CA").ok() else {
        info!("TLS not configured, serving plaintext");
        return Ok(None);
    };

    let cert = tokio::fs::read(&cert_path).await?;
    let key = tokio::fs::read(&key_path).await?;
    let ca = tokio::fs::read(&ca_path).await?;

    let config = ServerTlsConfig::new()
        .identity(Identity::from_pem(cert, key))
        .client_ca_root(Certificate::from_pem(ca));

    info!("mTLS enabled");
    Ok(Some(config))
}
