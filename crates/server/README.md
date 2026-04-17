# hermes-broker-server

[![Crates.io](https://img.shields.io/crates/v/hermes-broker-server.svg)](https://crates.io/crates/hermes-broker-server)
[![docs.rs](https://img.shields.io/docsrs/hermes-broker-server)](https://docs.rs/hermes-broker-server)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](https://github.com/GodlyJaaaj/hermes-rs#license)

gRPC server for the [Hermes message broker](https://github.com/GodlyJaaaj/hermes-rs) — both a library and a ready-to-run binary.

This crate provides `BrokerService`, the gRPC service implementation that bridges incoming streams (publish and subscribe) to the core routing engine (`hermes-broker-core`). You can use it as a **library** to embed the broker in your own application, or run it directly as a **standalone binary**. It also enables gRPC reflection so you can introspect the API with tools like `grpcurl`.

## Quick Start (Binary)

```bash
# Install and run
cargo install hermes-broker-server
hermes-broker-server

# Or run from source
cargo run -p hermes-broker-server
```

The server listens on `[::1]:50051` by default.

## Library Usage

You can embed the broker's gRPC service in your own application. This is useful if you want to bundle the broker alongside other services, customize the transport layer, or integrate with your own lifecycle management:

```rust
use hermes_broker_core::router::{Router, RouterConfig};
use hermes_broker_proto::broker_server::BrokerServer;
use hermes_broker_server::grpc::BrokerService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (router, router_tx) = Router::new(RouterConfig::default(), 8192);
    tokio::spawn(router.run());

    let service = BrokerService::new(router_tx);

    Server::builder()
        .add_service(BrokerServer::new(service))
        .serve("[::1]:50051".parse()?)
        .await?;

    Ok(())
}
```

## How It Works

When a client connects:

1. **Publish stream** — the server receives a stream of `PublishRequest` messages, converts each into a `RouterCmd::Publish`, and forwards it to the router. When the stream closes, the server replies with a `PublishAck` containing the total message count.
2. **Subscribe stream** — the server opens a bidirectional stream. Incoming `SubscribeRequest` commands register subscriptions with the router. The server then forwards matching deliveries back to the client. When the client disconnects, all its subscriptions are automatically cleaned up.

```
Client ◄──── gRPC (tonic) ────► BrokerService ──── mpsc ────► Router
                                                               (core)
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HERMES_ADDR` | `[::1]:50051` | Bind address for the gRPC listener |
| `HERMES_ROUTER_CAPACITY` | `8192` | Capacity of the router command mpsc channel (publish/subscribe backpressure threshold) |
| `RUST_LOG` | `info` | Log level filter ([tracing directives](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)) |
| `HERMES_LOG_FORMAT` | `text` | Set to `json` for structured JSON output (useful for containers and log aggregation) |
| `HERMES_TLS_CERT` | — | Path to PEM-encoded server certificate (enables mTLS when all three TLS vars are set) |
| `HERMES_TLS_KEY` | — | Path to PEM-encoded server private key |
| `HERMES_TLS_CA` | — | Path to PEM-encoded CA certificate (for verifying client certificates) |

### Logging Examples

```bash
# Debug logging for the router only
RUST_LOG=hermes_broker_core::router=debug hermes-broker-server

# Full trace logging
RUST_LOG=trace hermes-broker-server

# JSON output for containers / log aggregation
HERMES_LOG_FORMAT=json hermes-broker-server
```

### mTLS

To enable mutual TLS, set all three environment variables pointing to your PEM files:

```bash
HERMES_TLS_CERT=./certs/server.pem \
HERMES_TLS_KEY=./certs/server-key.pem \
HERMES_TLS_CA=./certs/ca.pem \
hermes-broker-server
```

When mTLS is enabled, clients must present a valid certificate signed by the same CA. If any of the three variables is missing, the server falls back to plaintext.

On the client side, configure `TlsConfig` when connecting:

```rust
use hermes_broker_client::{connect, TlsConfig};

let tls = TlsConfig {
    ca_cert: std::fs::read("certs/ca.pem")?,
    client_cert: std::fs::read("certs/client.pem")?,
    client_key: std::fs::read("certs/client-key.pem")?,
};
let channel = connect("https://[::1]:50051", Some(tls)).await?;
```

### Router Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broadcast_capacity` | 4096 | Broadcast channel buffer size per fanout slot |
| `queue_channel_capacity` | 256 | mpsc channel buffer size per queue group member |

## gRPC Reflection

The server registers gRPC reflection (v1), so you can explore the API with standard tools:

```bash
# List available services
grpcurl -plaintext [::1]:50051 list

# Describe the Broker service
grpcurl -plaintext [::1]:50051 describe hermes.broker.v1.Broker
```

## Testing

The crate includes integration tests that spin up a real broker and exercise publish/subscribe flows:

```bash
cargo test -p hermes-broker-server
```

Tests cover: exact subject matching, `*` and `>` wildcard routing, fanout delivery, queue group load balancing, and disconnect cleanup.

## Minimum Supported Rust Version

This crate requires **Rust 1.85+** (edition 2024).

## License

Licensed under either of [MIT](../../LICENSE-MIT) or [Apache-2.0](../../LICENSE-APACHE) at your option.
