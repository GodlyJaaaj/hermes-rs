# hermes-server

gRPC server crate for the **Hermes** broker.

This crate provides the server runtime: publishing, fanout / queue-group subscriptions, and durable mode (with storage) when enabled.

## Installation

Add the dependency:

```toml
[dependencies]
hermes-broker-server = "0.3"
tokio = { version = "1", features = ["full"] }
```

## Quick start

### 1) Start with default config

```rust
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:4222").await?;
    hermes_server::run(listener).await?;
    Ok(())
}
```

### 2) Start with custom config

```rust
use tokio::net::TcpListener;
use hermes_server::config::ServerConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:4222").await?;

    let config = ServerConfig {
        subscriber_channel_capacity: 4096,
        grpc_output_buffer: 256,
        store_path: Some("hermes.redb".into()),
        redelivery_interval_secs: 3,
        max_delivery_attempts: 10,
        retention_secs: 24 * 3600,
        default_ack_timeout_secs: 30,
        default_max_in_flight: 64,
        gc_interval_secs: 60,
        redelivery_batch_size: 200,
        ..ServerConfig::default()
    };

    hermes_server::run_with_config(listener, config).await?;
    Ok(())
}
```

## Configuration reference

All settings can be set programmatically via `ServerConfig` or through environment
variables with `ServerConfig::from_env()`.

### Network

| Env variable | Config field | Default | Description |
|---|---|---|---|
| `HERMES_LISTEN_ADDR` | `listen_addr` | `0.0.0.0:4222` | IP address and port the gRPC server binds to. |

### Buffers

| Env variable | Config field | Default | Description |
|---|---|---|---|
| `HERMES_CHANNEL_CAPACITY` | `subscriber_channel_capacity` | `8192` | Capacity of the main buffer between publishers and subscribers (broadcast channel for fanout, mpsc channel for queue groups). When a subscriber is slow, messages accumulate here. In fanout mode, exceeding this capacity causes the subscriber to lag and miss messages. |
| `HERMES_GRPC_OUTPUT_BUFFER` | `grpc_output_buffer` | `1024` | Per-subscriber buffer between the internal forwarding task and the gRPC output stream. Keep it small — back-pressure should be handled by the main channel above. |

### Durable storage

| Env variable | Config field | Default | Description |
|---|---|---|---|
| `HERMES_STORE_PATH` | `store_path` | `None` | Path to the redb database file. When set, durable mode is enabled (persistent messages, ack/nack, redelivery). When `None`, the server runs in fire-and-forget mode only. |

### Redelivery

These settings control the background loop that re-delivers messages that were
sent to a consumer but never acknowledged within the timeout window.

| Env variable | Config field | Default | Description |
|---|---|---|---|
| `HERMES_REDELIVERY_INTERVAL` | `redelivery_interval_secs` | `5` | How often (in seconds) the redelivery loop checks for expired messages. |
| `HERMES_MAX_DELIVERY_ATTEMPTS` | `max_delivery_attempts` | `5` | Maximum number of delivery attempts per message. After this many failures the message is dead-lettered. |
| `HERMES_REDELIVERY_BATCH_SIZE` | `redelivery_batch_size` | `100` | Maximum number of expired messages processed per consumer per redelivery cycle. Prevents a single cycle from monopolizing the CPU when thousands of messages have expired. |

### Ack / in-flight control

| Env variable | Config field | Default | Description |
|---|---|---|---|
| `HERMES_ACK_TIMEOUT` | `default_ack_timeout_secs` | `30` | Time (in seconds) a consumer has to acknowledge a message after receiving it. If the deadline passes without an ack, the message is considered expired and eligible for redelivery. Consumers can override this per-subscription in their `DurableSubscribeRequest`. |
| `HERMES_MAX_IN_FLIGHT` | `default_max_in_flight` | `32` | Maximum number of unacknowledged messages the broker will send to a single consumer at the same time. Limits pressure on slow consumers. Consumers can override this per-subscription. |

### Garbage collection

| Env variable | Config field | Default | Description |
|---|---|---|---|
| `HERMES_RETENTION_SECS` | `retention_secs` | `3600` | How long (in seconds) acknowledged messages are kept in the store before being deleted by the GC loop. Useful for short-term auditing. |
| `HERMES_GC_INTERVAL` | `gc_interval_secs` | `60` | How often (in seconds) the GC loop runs to purge old acknowledged messages. |

### Data flow

```
Publisher
   │
   ▼
┌──────────────────────────────────┐
│  subscriber_channel_capacity     │  ← main buffer (default 8192)
│  (broadcast for fanout,          │
│   mpsc for queue groups)         │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  grpc_output_buffer              │  ← per-subscriber gRPC buffer (default 1024)
│  (mpsc → tonic stream)           │
└──────────────┬───────────────────┘
               │
               ▼
           gRPC client

Background loops (durable mode only):

┌───────────────────┐  every 5s     ┌─────────────────────────┐
│  redelivery loop  ├──────────────►│ re-sends expired msgs   │
│                   │  ≤100/consumer│ (up to max_delivery_attempts) │
└───────────────────┘               └─────────────────────────┘

┌───────────────────┐  every 60s    ┌─────────────────────────┐
│  GC loop          ├──────────────►│ deletes acked msgs      │
│                   │  retention:1h │ older than retention     │
└───────────────────┘               └─────────────────────────┘
```

### Example

```bash
HERMES_LISTEN_ADDR=0.0.0.0:4222 \
HERMES_STORE_PATH=./hermes.redb \
HERMES_CHANNEL_CAPACITY=16384 \
HERMES_RETENTION_SECS=86400 \
HERMES_MAX_IN_FLIGHT=64 \
cargo run -p hermes-server
```

## Notes

- Without `HERMES_STORE_PATH` (or `store_path = None`), the server runs in fire-and-forget mode — no persistence, no redelivery, no GC.
- With a configured store, redelivery and GC loops are automatically spawned and stopped on graceful shutdown.
- The service exposes gRPC server reflection for API discovery (e.g. with `grpcurl`).