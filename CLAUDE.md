# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

**Prerequisite**: `protoc` (protobuf compiler) must be installed for proto compilation.

```sh
cargo check --workspace          # Type-check all crates
cargo build --workspace          # Build all crates
cargo test --workspace           # Run all tests
cargo test -p hermes-integration-tests  # Integration tests only
cargo clippy --workspace -- -D warnings # Lint (CI treats warnings as errors)
cargo fmt --all -- --check       # Format check
cargo fmt --all                  # Auto-format

cargo run -p hermes-broker-server       # Run the broker server
cargo bench -p hermes-server            # In-process engine benchmarks
cargo bench -p hermes-integration-tests # End-to-end gRPC benchmarks
```

After cloning, install the pre-push hook (enforces `cargo fmt --check`):
```sh
./hooks/install.sh
```

## Architecture

Hermes is an async event broker for Rust built on gRPC (tonic) and tokio. It supports fire-and-forget fanout, queue groups (load-balanced), and durable delivery with ack/nack.

### Workspace Crates (edition 2024, resolver 3)

| Crate | Package name | Role |
|-------|-------------|------|
| `crates/macros` | hermes-broker-macros | `#[derive(Event)]` and `event_group!` proc macros |
| `crates/proto` | hermes-broker-proto | Protobuf/gRPC service definitions and generated types (`proto/hermes/broker/v1/broker.proto`) |
| `crates/core` | hermes-broker-core | `Event` trait, `Subject`/`Segment` types, bincode encode/decode helpers |
| `crates/store` | hermes-broker-store | `MessageStore` trait + `RedbMessageStore` impl (redb embedded DB) |
| `crates/server` | hermes-broker-server | gRPC broker: `BrokerEngine` (routing), `BrokerService` (tonic impl), redelivery/GC loops |
| `crates/client` | hermes-broker-client | `HermesClient`, `BatchPublisher`, `DurableSubscriber` |
| `crates/integration-tests` | (not published) | Integration tests, benchmarks, examples |

### Dependency order (publish order)

macros -> core -> proto -> store -> server -> client

### Key Design Decisions

- **Subject-based routing**: Events are routed by `Subject` (ordered segments: `Str`, `Int`, `Any`/`*`, `Rest`/`>`). Exact-match is O(1) via DashMap; wildcard subscriptions are checked as fallback.
- **Wire format**: Subjects and event payloads use **bincode** (serde) serialization. The proto `EventEnvelope` carries subject and payload as raw bytes.
- **Fanout**: Uses tokio `broadcast` channel with `Arc<EventEnvelope>` for zero-copy delivery to all subscribers.
- **Queue groups**: Round-robin dispatch via per-group `mpsc` channels and a global atomic counter.
- **Durable mode**: Enabled by setting `HERMES_STORE_PATH`. Messages are persisted to redb before dispatch. Background loops handle redelivery of expired messages and GC of old acked messages.
- **Server config**: All settings configurable via env vars (`HERMES_STORE_PATH`, `HERMES_ACK_TIMEOUT`, `HERMES_MAX_DELIVERY_ATTEMPTS`, etc.) or `ServerConfig` struct.

### Server Internal Flow

```
gRPC request -> BrokerService (grpc.rs)
  -> BrokerEngine (broker.rs)
    -> exact_subscriptions (DashMap<Subject, SubjectSubscribers>)
    -> wildcard_subscriptions (DashMap<Subject, WildcardEntry>)
    -> durable_consumers (DashMap<String, DurableConsumer>)
    -> [optional] MessageStore (redb)
```

### gRPC Service (4 RPCs)

- `Publish`: client-streaming, fire-and-forget
- `Subscribe`: server-streaming, fire-and-forget
- `PublishDurable`: client-streaming, persisted
- `SubscribeDurable`: bidirectional streaming, ack/nack protocol
