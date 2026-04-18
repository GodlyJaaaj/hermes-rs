# hermes-broker-proto

[![Crates.io](https://img.shields.io/crates/v/hermes-broker-proto.svg)](https://crates.io/crates/hermes-broker-proto)
[![docs.rs](https://img.shields.io/docsrs/hermes-broker-proto)](https://docs.rs/hermes-broker-proto)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](https://github.com/GodlyJaaaj/hermes-rs#license)

Protobuf and gRPC service definitions for the [Hermes message broker](https://github.com/GodlyJaaaj/hermes-rs).

This crate contains the `.proto` source file and the generated Rust types (via `tonic` + `prost`). All types are re-exported at the crate root for convenience.

## gRPC Service

The `Broker` service exposes two streaming RPCs:

```protobuf
service Broker {
  // Client-streaming: fire-and-forget message publishing.
  // Client sends PublishRequest messages, receives a PublishAck with total count when done.
  rpc Publish(stream PublishRequest) returns (PublishAck);

  // Bidirectional streaming: subscribe to subjects and receive matching deliveries.
  rpc Subscribe(stream SubscribeRequest) returns (stream SubscribeResponse);
}
```

## Message Types

| Type | Fields | Description |
|------|--------|-------------|
| `PublishRequest` | `subject`, `payload`, `reply_to` | A message to publish. `reply_to` is optional (empty string = not set). |
| `PublishAck` | `total_published` | Returned when the publish stream closes. |
| `SubscribeRequest` | `sub` | A subscription command containing a `Sub`. |
| `Sub` | `subject`, `queue_group` | Subject pattern to subscribe to. `queue_group` enables load-balanced delivery. |
| `Message` | `subject`, `payload`, `sequence`, `reply_to` | A delivered message with a monotonically increasing sequence number. |
| `SubscribeResponse` | `messages` (repeated `Message`) | One or more deliveries grouped into a single frame. The server coalesces deliveries that are ready at the same time, amortizing per-frame overhead on the fanout hot path. The client splits the batch transparently. |

## Usage

This crate is a dependency of `hermes-broker-client` and `hermes-broker-server` — you typically don't need to depend on it directly unless you're building a custom client or server.

```toml
[dependencies]
hermes-broker-proto = "0.7"
```

All generated types are available at the crate root:

```rust
use hermes_broker_proto::{Message, PublishRequest, SubscribeRequest, SubscribeResponse, Sub};
use hermes_broker_proto::broker_client::BrokerClient;
use hermes_broker_proto::broker_server::{Broker, BrokerServer};
```

## gRPC Reflection

The crate exports a `FILE_DESCRIPTOR_SET` constant for use with [tonic-reflection](https://docs.rs/tonic-reflection), enabling runtime introspection of the gRPC API:

```rust
use hermes_broker_proto::FILE_DESCRIPTOR_SET;

let reflection = tonic_reflection::server::Builder::configure()
    .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
    .build_v1()?;
```

## Minimum Supported Rust Version

This crate requires **Rust 1.85+** (edition 2024).

## License

Licensed under either of [MIT](../../LICENSE-MIT) or [Apache-2.0](../../LICENSE-APACHE) at your option.
