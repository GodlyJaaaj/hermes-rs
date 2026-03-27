//! Simple pub/sub example.
//!
//! 1. Start the broker: `cargo run -p scylla-broker-server`
//! 2. In another terminal: `cargo run -p scylla-broker-client --example simple_pubsub`

use futures::StreamExt;
use scylla_broker_client::ScyllaBrokerClient;
use scylla_broker_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct ChatMessage {
    user: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ScyllaBrokerClient::connect("http://127.0.0.1:4222").await?;

    // Spawn subscriber
    let sub_client = client.clone();
    let sub_handle = tokio::spawn(async move {
        let mut stream = sub_client.subscribe::<ChatMessage>(&[]).await.unwrap();
        println!("Listening for ChatMessage events...");
        while let Some(result) = stream.next().await {
            match result {
                Ok(msg) => println!("[{}]: {}", msg.user, msg.text),
                Err(e) => eprintln!("Error: {e}"),
            }
        }
    });

    // Give subscriber time to connect
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publish some messages
    for i in 0..5 {
        client
            .publish(&ChatMessage {
                user: "alice".into(),
                text: format!("Hello #{i}!"),
            })
            .await?;
        println!("Published message #{i}");
    }

    // Wait a bit for delivery
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    sub_handle.abort();

    println!("Done!");
    Ok(())
}
