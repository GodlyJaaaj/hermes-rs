//! Client library for the Hermes message broker.
//!
//! Provides [`Publisher`] for fire-and-forget message publishing and [`Subscriber`]
//! for receiving messages over a gRPC stream.
//!
//! # Quick start
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use hermes_broker_client::{connect, Publisher, Subscriber};
//!
//! // Plaintext (dev / testing)
//! let channel = connect("http://[::1]:50051", None).await?;
//!
//! // mTLS (production)
//! // let tls = hermes_broker_client::TlsConfig {
//! //     ca_cert: std::fs::read("ca.pem")?,
//! //     client_cert: std::fs::read("client.pem")?,
//! //     client_key: std::fs::read("client-key.pem")?,
//! // };
//! // let channel = connect("https://[::1]:50051", Some(tls)).await?;
//!
//! // Publish
//! let publisher = Publisher::new(channel.clone());
//! publisher.publish("orders.eu.created", &b"hello"[..]).await?;
//!
//! // Subscribe
//! let mut subscriber = Subscriber::new(channel).await?;
//! subscriber.subscribe("orders.>", None).await?;
//! while let Some(msg) = subscriber.recv().await {
//!     println!("{}: {:?}", msg.subject, msg.payload);
//! }
//! # Ok(())
//! # }
//! ```

mod publisher;
mod subscriber;

pub use publisher::Publisher;
pub use subscriber::Subscriber;

use hermes_proto::broker_client::BrokerClient;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tracing::{debug, info};

/// TLS configuration for mTLS connections.
pub struct TlsConfig {
    /// PEM-encoded CA certificate (to verify the server).
    pub ca_cert: Vec<u8>,
    /// PEM-encoded client certificate.
    pub client_cert: Vec<u8>,
    /// PEM-encoded client private key.
    pub client_key: Vec<u8>,
}

/// Connect to a hermes broker and return a channel for creating publishers/subscribers.
///
/// Pass `None` for plaintext (dev/test) or `Some(TlsConfig)` for mTLS.
pub async fn connect(
    addr: &str,
    tls: Option<TlsConfig>,
) -> Result<Channel, tonic::transport::Error> {
    debug!(addr, tls = tls.is_some(), "connecting to hermes broker");
    let mut endpoint = Channel::from_shared(addr.to_string())
        .expect("invalid address")
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(10))
        .keep_alive_timeout(std::time::Duration::from_secs(5))
        .http2_keep_alive_interval(std::time::Duration::from_secs(10));

    if let Some(tls) = tls {
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(tls.ca_cert))
            .identity(Identity::from_pem(tls.client_cert, tls.client_key))
            .domain_name("localhost");
        endpoint = endpoint.tls_config(tls_config)?;
    }

    let channel = endpoint.connect().await?;
    info!(addr, "connected to hermes broker");
    Ok(channel)
}

/// Create a raw gRPC client (useful for advanced usage).
pub fn raw_client(channel: Channel) -> BrokerClient<Channel> {
    BrokerClient::new(channel)
}
