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

## Configuration via environment variables

You can build config from the environment with `ServerConfig::from_env()`.

Supported variables:

- `HERMES_LISTEN_ADDR` (e.g. `0.0.0.0:4222`)
- `HERMES_CHANNEL_CAPACITY`
- `HERMES_GRPC_OUTPUT_BUFFER`
- `HERMES_STORE_PATH` (enables durable mode)
- `HERMES_REDELIVERY_INTERVAL`
- `HERMES_MAX_DELIVERY_ATTEMPTS`
- `HERMES_RETENTION_SECS`
- `HERMES_ACK_TIMEOUT`
- `HERMES_MAX_IN_FLIGHT`
- `HERMES_GC_INTERVAL`
- `HERMES_REDELIVERY_BATCH_SIZE`

Example:

```bash
HERMES_LISTEN_ADDR=0.0.0.0:4222 \
HERMES_STORE_PATH=./hermes.redb \
HERMES_RETENTION_SECS=86400 \
cargo run -p hermes-server
```

## Notes

- Without `HERMES_STORE_PATH` (or `store_path = None`), the server runs in fire-and-forget mode.
- With a configured store, redelivery and GC loops are automatically enabled.
- The service also exposes gRPC reflection for API inspection.