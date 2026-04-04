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
//! let channel = connect("http://[::1]:50051").await?;
//!
//! // Publish
//! let publisher = Publisher::new(channel.clone()).await?;
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
use tonic::transport::Channel;
use tracing::{debug, info};

/// Connect to a hermes broker and return a channel for creating publishers/subscribers.
pub async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
    debug!(addr, "connecting to hermes broker");
    let channel = Channel::from_shared(addr.to_string())
        .expect("invalid address")
        .connect()
        .await?;
    info!(addr, "connected to hermes broker");
    Ok(channel)
}

/// Create a raw gRPC client (useful for advanced usage).
pub fn raw_client(channel: Channel) -> BrokerClient<Channel> {
    BrokerClient::new(channel)
}
