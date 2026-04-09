# hermes-broker-client

[![Crates.io](https://img.shields.io/crates/v/hermes-broker-client.svg)](https://crates.io/crates/hermes-broker-client)
[![docs.rs](https://img.shields.io/docsrs/hermes-broker-client)](https://docs.rs/hermes-broker-client)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](https://github.com/GodlyJaaaj/hermes-rs#license)

gRPC client library for the [Hermes message broker](https://github.com/GodlyJaaaj/hermes-rs) — publish and subscribe to messages with fire-and-forget semantics and automatic reconnection.

Hermes is a lightweight, high-performance message broker built in Rust. It uses trie-based subject routing with dot-separated topics (like `orders.eu.created`), supports broadcast fanout and load-balanced queue groups, and communicates over gRPC streaming.

This crate is a library that provides the two main building blocks for interacting with a running broker: `Publisher` and `Subscriber`. Add it as a dependency to your project and use it to publish and consume messages from any Rust application.

## Features

- **Fire-and-forget publishing** — messages are queued and streamed, no per-message ack overhead
- **Automatic reconnection** — the publisher transparently reconnects on stream failure (2 s backoff)
- **Wildcard subscriptions** — `*` matches one token, `>` matches one or more trailing tokens
- **Queue groups** — load-balanced round-robin delivery across group members
- **mTLS support** — optional mutual TLS for production deployments

## Getting Started

Add the dependency:

```bash
cargo add hermes-broker-client
```

Or in your `Cargo.toml`:

```toml
[dependencies]
hermes-broker-client = "0.6"
```

You'll also need a running Hermes broker — see [`hermes-broker-server`](https://crates.io/crates/hermes-broker-server).

## Publisher Example

A `Publisher` streams messages to the broker with fire-and-forget semantics — `publish()` returns as soon as the message is queued locally, without waiting for a broker acknowledgment. If the connection drops (broker restart, network hiccup), the publisher automatically reconnects in the background with a 2-second backoff.

```rust
use bytes::Bytes;
use hermes_broker_client::{connect, Publisher};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = connect("http://[::1]:50051", None).await?;
    let publisher = Publisher::new(channel);

    publisher.publish("orders.eu.created", Bytes::from("order-123")).await?;
    publisher.publish("orders.us.created", Bytes::from("order-456")).await?;

    Ok(())
}
```

> **Tip:** `Publisher::new` is not async — it spawns a background tokio task and returns immediately.

## Subscriber Example

A `Subscriber` opens a bidirectional gRPC stream with the broker. You can subscribe to one or more subjects (including wildcard patterns), then receive matching messages via `recv()`. Dropping the subscriber closes the stream and triggers server-side cleanup.

```rust
use hermes_broker_client::{connect, Subscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = connect("http://[::1]:50051", None).await?;
    let mut subscriber = Subscriber::new(channel).await?;

    // Subscribe to all order events using the `>` wildcard
    // (matches `orders.eu.created`, `orders.us.deleted.v2`, etc.)
    subscriber.subscribe("orders.>", None).await?;

    while let Some(msg) = subscriber.recv().await {
        println!(
            "[seq={}] {}: {}",
            msg.sequence,
            msg.subject,
            String::from_utf8_lossy(&msg.payload),
        );
    }

    Ok(())
}
```

Subjects are dot-separated tokens. Wildcards are only valid in subscriptions:

| Pattern | Matches | Doesn't match |
|---------|---------|---------------|
| `orders.eu.created` | `orders.eu.created` | `orders.us.created` |
| `orders.*.created` | `orders.eu.created`, `orders.us.created` | `orders.eu.deleted` |
| `orders.>` | `orders.eu`, `orders.eu.created` | `orders` |

## Queue Groups

By default, all subscribers on a subject receive every message (fanout / broadcast). If you want to distribute work across multiple consumers instead, subscribe with a queue group name — the broker delivers each message to exactly one member of the group using round-robin.

```rust
// Both subscribers share the "workers" queue group.
// A message published to "jobs.process" goes to exactly one of them.
subscriber1.subscribe("jobs.process", Some("workers".into())).await?;
subscriber2.subscribe("jobs.process", Some("workers".into())).await?;
```

This is useful for horizontally scaling consumers: spin up more instances with the same group name and the broker balances the load automatically.

## Request / Reply

Hermes is primarily a fire-and-forget broker, but you can implement request/reply on top of it. The pattern works like this: the requester subscribes to a unique "inbox" subject, publishes a message with `reply_to` set to that inbox, and the responder publishes its reply back to the `reply_to` subject.

```rust
use bytes::Bytes;
use hermes_broker_client::{connect, Publisher, Subscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = connect("http://[::1]:50051", None).await?;
    let publisher = Publisher::new(channel.clone());
    let mut subscriber = Subscriber::new(channel).await?;

    // 1. Subscribe to a unique inbox for the reply
    subscriber.subscribe("inbox.client-42", None).await?;

    // 2. Send a request, telling the responder where to reply
    publisher.publish_with_reply(
        "rpc.get-user",
        Bytes::from(r#"{"id": 42}"#),
        "inbox.client-42",
    ).await?;

    // 3. Wait for the reply
    if let Some(reply) = subscriber.recv().await {
        println!("got reply: {}", String::from_utf8_lossy(&reply.payload));
    }

    Ok(())
}
```

## API Overview

| Type | Description |
|------|-------------|
| [`connect()`](https://docs.rs/hermes-broker-client/latest/hermes_broker_client/fn.connect.html) | Connect to a broker (plaintext or mTLS) |
| [`Publisher`](https://docs.rs/hermes-broker-client/latest/hermes_broker_client/struct.Publisher.html) | Fire-and-forget publisher with auto-reconnect |
| [`Subscriber`](https://docs.rs/hermes-broker-client/latest/hermes_broker_client/struct.Subscriber.html) | Subscribe to subjects and receive messages |
| [`TlsConfig`](https://docs.rs/hermes-broker-client/latest/hermes_broker_client/struct.TlsConfig.html) | mTLS configuration (CA cert, client cert & key) |
| `PublishError` | Returned when the publisher is disconnected |
| `SubscribeError` | Returned when the subscriber is disconnected |

## Minimum Supported Rust Version

This crate requires **Rust 1.85+** (edition 2024).

## License

Licensed under either of [MIT](../../LICENSE-MIT) or [Apache-2.0](../../LICENSE-APACHE) at your option.
