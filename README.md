# hermes-rs

[![CI](https://github.com/GodlyJaaaj/hermes-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/GodlyJaaaj/hermes-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/hermes-broker-client.svg)](https://crates.io/crates/hermes-broker-client)
[![docs.rs](https://docs.rs/hermes-broker-client/badge.svg)](https://docs.rs/hermes-broker-client)
[![License](https://img.shields.io/crates/l/hermes-broker-client.svg)](LICENSE-MIT)

**Hermes** is a lightweight, async event broker for Rust built on gRPC. It supports **fire-and-forget** fanout, **queue groups** for load-balanced consumption, and **durable delivery** with ack/nack and automatic redelivery.

## Features

- **Typed events** — define events as Rust structs with `#[derive(Event)]`, serialized automatically
- **Fanout** — every subscriber receives every message
- **Queue groups** — competing consumers, each message delivered to exactly one subscriber per group
- **Event groups** — subscribe to multiple event types as a single merged stream
- **Durable delivery** — at-least-once with persistence, ack/nack, redelivery, and dead-lettering
- **Batch publishing** — high-throughput buffered publisher
- **Wildcard subscriptions** — `*` (single segment) and `>` (rest) pattern matching
- **gRPC transport** — built on [tonic](https://crates.io/crates/tonic), HTTP/2 multiplexed

## Quick start

### 1. Start the broker

```rust,no_run
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:4222").await?;
    hermes_server::run(listener).await?;
    Ok(())
}
```

Or run directly:

```sh
cargo run -p hermes-broker-server
```

### 2. Publish and subscribe

```rust,no_run
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
#[event(subject = "chat.message")]
struct ChatMessage {
    user: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;

    // Subscribe FIRST — fanout delivers only to active subscribers
    let sub = client.clone();
    tokio::spawn(async move {
        let mut stream = sub.subscribe::<ChatMessage>(&[]).await.unwrap();
        while let Some(Ok(msg)) = stream.next().await {
            println!("[{}]: {}", msg.user, msg.text);
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publish
    client.publish(&ChatMessage {
        user: "alice".into(),
        text: "Hello, world!".into(),
    }).await?;

    Ok(())
}
```

### 3. Queue groups (load balancing)

```rust,no_run
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct Task { id: u32 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;

    // Multiple subscribers in the same group — each message goes to exactly one
    for i in 0..3 {
        let c = client.clone();
        tokio::spawn(async move {
            let mut stream = c.subscribe::<Task>(&["workers"]).await.unwrap();
            while let Some(Ok(task)) = stream.next().await {
                println!("Worker {i} got task #{}", task.id);
            }
        });
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    for id in 0..10 {
        client.publish(&Task { id }).await?;
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}
```

### 4. Durable delivery (at-least-once)

```rust,no_run
use hermes_client::HermesClient;
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct Payment { id: String }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;

    // Publish with persistence
    client.publish_durable(&Payment { id: "PAY-1".into() }).await?;

    // Subscribe with ack/nack — un-acked messages are automatically redelivered
    let mut sub = client
        .subscribe_durable::<Payment>("payment-processor", &[], 10, 30)
        .await?;

    while let Some(msg) = sub.next().await {
        let msg = msg?;
        println!("payment: {:?}", msg.event);
        msg.ack().await?;
    }

    Ok(())
}
```

## Crates

| Crate | Description |
|-------|-------------|
| [`hermes-broker-client`](https://crates.io/crates/hermes-broker-client) | Typed async client — publish, subscribe, durable, batch |
| [`hermes-broker-server`](https://crates.io/crates/hermes-broker-server) | Broker server with fanout, queue groups, durable mode |
| [`hermes-broker-core`](https://crates.io/crates/hermes-broker-core) | Event trait, subject types, encoding helpers |
| [`hermes-broker-proto`](https://crates.io/crates/hermes-broker-proto) | Protobuf/gRPC generated types and services |
| [`hermes-broker-store`](https://crates.io/crates/hermes-broker-store) | Durable message storage backend (redb) |
| [`hermes-broker-macros`](https://crates.io/crates/hermes-broker-macros) | `#[derive(Event)]` and `event_group!` macros |

## Server configuration

Configure via environment variables or `ServerConfig` struct:

```sh
HERMES_STORE_PATH=./hermes.redb \   # enables durable mode
HERMES_ACK_TIMEOUT=30 \
HERMES_MAX_DELIVERY_ATTEMPTS=5 \
cargo run -p hermes-broker-server
```

Without `HERMES_STORE_PATH`, the server runs in fire-and-forget mode only.

See the [server README](crates/server/README.md) for all configuration options.

## Development

After cloning, install the git hooks:

```sh
./hooks/install.sh
```

This sets up a **pre-push** hook that runs `cargo fmt --check` and blocks the push if code is unformatted.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
