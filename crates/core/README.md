# hermes-broker-core

[![Crates.io](https://img.shields.io/crates/v/hermes-broker-core.svg)](https://crates.io/crates/hermes-broker-core)
[![docs.rs](https://img.shields.io/docsrs/hermes-broker-core)](https://docs.rs/hermes-broker-core)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](https://github.com/GodlyJaaaj/hermes-rs#license)

Core routing engine for the [Hermes message broker](https://github.com/GodlyJaaaj/hermes-rs) — trie-based subject matching with fanout and queue group delivery.

This crate contains the inner workings of the broker: the `Router` task, the `TrieNode` subject index, and the `SlotMap` that manages subscriber channels. It is intentionally transport-agnostic — it knows nothing about gRPC or networking. The server crate (`hermes-broker-server`) bridges gRPC streams to the router via a command channel.

## How It Works

### Subject Matching

Subjects are dot-separated tokens (e.g. `orders.eu.created`). Subscriptions support two wildcards:

- `*` — matches exactly **one** token (`orders.*.created` matches `orders.eu.created` and `orders.us.created`)
- `>` — matches **one or more** trailing tokens (`orders.>` matches `orders.eu`, `orders.eu.created`, etc.)

The trie indexes these patterns so that a published message is matched against all subscriptions in a single traversal, with O(k) complexity where k is the number of tokens in the subject.

### Delivery Modes

When a message matches a subscription, the router delivers it in one of two ways:

- **Fanout (broadcast)** — every subscriber on the subject receives a copy of every matching message. Backed by a tokio `broadcast` channel. This is the default when subscribing without a queue group.
- **Queue group (load-balanced)** — messages are distributed round-robin across group members. Backed by tokio `mpsc` channels. Useful for horizontally scaling consumers.

### Single-Threaded, Lock-Free Design

The `Router` runs as a single async task that owns all state (trie + slot map). External components communicate with it exclusively through an `mpsc` channel of `RouterCmd` messages. This design avoids locks entirely — the router is the sole owner of the routing state.

## Architecture

```
                    ┌─────────────────────┐
   RouterCmd ──────►│      Router         │
   (mpsc)           │                     │
                    │  ┌───────────────┐  │
                    │  │   TrieNode    │  │
                    │  │  (subject     │  │
                    │  │   matching)   │  │
                    │  └───────┬───────┘  │
                    │          │          │
                    │  ┌───────▼───────┐  │
                    │  │   SlotMap     │  │
                    │  │  (channels)   │  │
                    │  └───────────────┘  │
                    └─────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              Broadcast            mpsc (queue)
              (fanout)             (round-robin)
```

## Usage

The core crate is consumed primarily by `hermes-broker-server`, but you can use it directly to embed routing logic in your own application:

```rust
use hermes_broker_core::router::{Router, RouterCmd, RouterConfig};
use hermes_broker_core::slot::SubHandle;
use bytes::Bytes;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    // Create and spawn the router
    let (router, tx) = Router::new(RouterConfig::default(), 1024);
    tokio::spawn(router.run());

    // Subscribe to a subject
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(RouterCmd::Subscribe {
        subject: Box::from("orders.>"),
        queue_group: None,
        reply: reply_tx,
    }).await.unwrap();

    let handle = reply_rx.await.unwrap();

    // Publish a message
    tx.send(RouterCmd::Publish {
        subject: Box::from("orders.eu.created"),
        payload: Bytes::from("hello"),
        reply_to: None,
    }).await.unwrap();

    // Receive the delivery
    match handle {
        SubHandle::Fanout { mut rx, .. } => {
            let delivery = rx.recv().await.unwrap();
            println!(
                "[seq={}] {}: {:?}",
                delivery.sequence,
                delivery.subject,
                delivery.payload,
            );
        }
        SubHandle::QueueMember { mut rx, .. } => {
            let delivery = rx.recv().await.unwrap();
            println!("got: {:?}", delivery);
        }
    }
}
```

## Router Commands

The router accepts three commands via its `mpsc` channel:

| Command | Description |
|---------|-------------|
| `Publish { subject, payload, reply_to }` | Route a message to all matching subscribers. Fire-and-forget — no response. |
| `Subscribe { subject, queue_group, reply }` | Register a subscription. Returns a `SubHandle` via oneshot for receiving deliveries. |
| `Disconnect { sub_id }` | Remove a subscriber from all its slots. Cleans up empty slots from the trie. |

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broadcast_capacity` | 4096 | Buffer size for broadcast channels (fanout slots). Lagged receivers skip messages. |
| `queue_channel_capacity` | 256 | Buffer size per queue group member (mpsc channel). Back-pressure applies when full. |

## Benchmarks

The crate includes end-to-end throughput benchmarks using [Criterion](https://docs.rs/criterion):

```bash
cargo bench -p hermes-broker-core
```

Three scenarios are benchmarked:
- **Fanout** — publish to 1, 10, 100, and 1,000 subscribers simultaneously
- **Queue group** — round-robin across 4 members
- **Wildcard routing** — `*` pattern matching with a single subscriber

## Minimum Supported Rust Version

This crate requires **Rust 1.85+** (edition 2024).

## License

Licensed under either of [MIT](../../LICENSE-MIT) or [Apache-2.0](../../LICENSE-APACHE) at your option.
