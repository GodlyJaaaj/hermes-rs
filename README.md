# hermes-rs

A lightweight, high-performance message broker built in Rust with gRPC streaming.

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE)

## Architecture

```
┌──────────────────────────────────┐
│  Client (hermes-broker-client)   │
│  Publisher / Subscriber          │
└──────────┬───────────────────────┘
           │ gRPC (tonic)
           ▼
┌──────────────────────────────────┐
│  Server (hermes-broker-server)   │
│  BrokerService                   │
└──────────┬───────────────────────┘
           │ mpsc channel
           ▼
┌──────────────────────────────────┐
│  Router (hermes-broker-core)     │
│  Trie (subject matching)         │
│  SlotMap (fanout + queue groups) │
└──────────────────────────────────┘
```

## Crates

| Crate | Description |
|-------|-------------|
| `hermes-broker-proto` | Protobuf definitions and generated gRPC code |
| `hermes-broker-core` | Routing engine — trie-based subject matching, fanout and queue groups |
| `hermes-broker-server` | gRPC server binary |
| `hermes-broker-client` | Client library — `Publisher` and `Subscriber` |

## Features

- **Subject routing** — dot-separated subjects (`orders.eu.created`) with trie-based matching
- **Wildcards** — `*` matches one token, `>` matches one or more trailing tokens
- **Fanout** — all subscribers on a subject receive every message (broadcast)
- **Queue groups** — load-balanced round-robin delivery across group members
- **Fire-and-forget publish** — client-streaming RPC, no per-message ack overhead
- **Structured logging** — `tracing` with env-configurable levels and JSON output
- **Single-threaded router** — lock-free by design, all routing in one async task

## Quick Start

```bash
# Run the broker
cargo run -p hermes-broker-server
```

The server listens on `[::1]:50051` by default.

## Examples

### Publisher

```rust
use bytes::Bytes;
use hermes_client::{connect, Publisher};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = connect("http://[::1]:50051").await?;
    let publisher = Publisher::new(channel).await?;

    publisher.publish("orders.eu.created", Bytes::from("order-123")).await?;
    publisher.publish("orders.us.created", Bytes::from("order-456")).await?;

    Ok(())
}
```

### Subscriber

```rust
use hermes_client::{connect, Subscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = connect("http://[::1]:50051").await?;
    let mut subscriber = Subscriber::new(channel).await?;

    subscriber.subscribe("orders.>", None).await?;

    while let Some(msg) = subscriber.recv().await {
        println!(
            "[seq={}] {}: {}",
            msg.sequence,
            msg.subject,
            String::from_utf8_lossy(&msg.payload)
        );
    }

    Ok(())
}
```

## Subject Wildcards

Subjects are dot-separated tokens. Wildcards are only valid in subscriptions, not in published subjects.

| Pattern | Matches | Doesn't match |
|---------|---------|---------------|
| `orders.eu.created` | `orders.eu.created` | `orders.us.created` |
| `orders.*.created` | `orders.eu.created`, `orders.us.created` | `orders.eu.deleted`, `orders.created` |
| `orders.>` | `orders.eu`, `orders.eu.created`, `orders.us.deleted.v2` | `orders` |

- `*` — matches exactly **one** token
- `>` — matches **one or more** trailing tokens (must be the last token)

## Queue Groups

By default, all subscribers on the same subject receive every message (fanout). To load-balance instead, subscribe with a queue group name:

```rust
// Both subscribers share the "workers" group — each message goes to exactly one
subscriber1.subscribe("jobs.process", Some("workers".into())).await?;
subscriber2.subscribe("jobs.process", Some("workers".into())).await?;
```

Messages are distributed round-robin across group members.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RUST_LOG` | `info` | Log level filter ([`tracing` directives](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)) |
| `HERMES_LOG_FORMAT` | text | Set to `json` for structured JSON output |

### Examples

```bash
# Debug logging for the router only
RUST_LOG=hermes_broker_core::router=debug cargo run -p hermes-broker-server

# Full trace logging
RUST_LOG=trace cargo run -p hermes-broker-server

# JSON output (for containers / log aggregation)
HERMES_LOG_FORMAT=json cargo run -p hermes-broker-server
```

### Router Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broadcast_capacity` | 4096 | Broadcast channel buffer size per fanout slot |
| `queue_channel_capacity` | 256 | mpsc channel buffer size per queue group member |

## gRPC API

Defined in [`broker.proto`](crates/proto/proto/hermes/broker/v1/broker.proto):

```protobuf
service Broker {
  rpc Publish(stream PublishRequest) returns (PublishAck);
  rpc Subscribe(stream SubscribeRequest) returns (stream SubscribeResponse);
}
```

- **Publish** — client-streaming. Send messages, receive total count when the stream closes.
- **Subscribe** — bidirectional streaming. Send subscribe commands, receive matching deliveries.

## Building & Testing

```bash
cargo build --workspace          # Build all crates
cargo test --workspace           # Run all tests (19 unit + 6 integration + 1 doctest)
cargo clippy --workspace         # Lint
cargo bench -p hermes-broker-core  # Run throughput benchmarks
```

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
