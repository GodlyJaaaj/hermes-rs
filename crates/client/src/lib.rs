mod publisher;
mod subscriber;

pub use publisher::Publisher;
pub use subscriber::Subscriber;

use hermes_proto::broker_client::BrokerClient;
use tonic::transport::Channel;

/// Connect to a hermes broker and return a channel for creating publishers/subscribers.
pub async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
    let channel = Channel::from_shared(addr.to_string())
        .expect("invalid address")
        .connect()
        .await?;
    Ok(channel)
}

/// Create a raw gRPC client (useful for advanced usage).
pub fn raw_client(channel: Channel) -> BrokerClient<Channel> {
    BrokerClient::new(channel)
}
