# hermes-client

Asynchronous Rust client to publish and consume events through the Hermes gRPC broker.

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
hermes-broker-client = "0.2"
hermes-broker-core = "0.2"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures = "0.3"
```

## Quickstart

Minimal example: define an event with `derive(Event)`, subscribe, then publish.

> **Note:** In fanout mode (no queue groups), subscribers only receive messages
> published **after** they connect. Always start the subscriber before publishing.

```rust
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct UserCreated {
    id: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;

    // 1) Subscribe first — fanout delivers only to active subscribers
    let sub_client = client.clone();
    let handle = tokio::spawn(async move {
        let mut stream = sub_client.subscribe::<UserCreated>(&[]).await.unwrap();
        while let Some(msg) = stream.next().await {
            println!("received: {:?}", msg.unwrap());
        }
    });

    // 2) Small delay so the subscription is registered on the broker
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 3) Publish
    client
        .publish(&UserCreated {
            id: "u_123".into(),
            email: "hello@acme.dev".into(),
        })
        .await?;

    // Wait a bit for delivery, then exit
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    handle.abort();

    Ok(())
}
```

## Event subjects with `derive(Event)`

By default, `#[derive(Event)]` generates a subject from module path + type name.

You can override the subject explicitly:

```rust
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
#[event(subject = "users.created")]
struct UserCreated {
    id: String,
}
```

## Event groups with `event_group!`

To subscribe to multiple event types as a single stream, create an `EventGroup` using `event_group!`:

```rust
use hermes_core::{event_group, Event};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct OrderCreated {
    order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct OrderShipped {
    order_id: String,
    tracking_id: String,
}

event_group!(OrderEvents = [OrderCreated, OrderShipped]);
```

Then subscribe to the group:

```rust
use futures::StreamExt;
use hermes_client::HermesClient;

// let mut stream = client.subscribe_group::<OrderEvents>(&[]).await?;
// while let Some(item) = stream.next().await {
//     match item? {
//         OrderEvents::OrderCreated(evt) => println!("created: {}", evt.order_id),
//         OrderEvents::OrderShipped(evt) => println!("shipped: {}", evt.tracking_id),
//     }
// }
```

## Core API

- `HermesClient::connect(...)`: connect to broker
- `publish(...)`: fire-and-forget publish
- `subscribe::<E>(...)`: typed subscription
- `subscribe_group::<G>(...)`: event-group subscription
- `publish_durable(...)`: durable publish
- `subscribe_durable::<E>(...)`: durable subscription (at-least-once)
- `publish_raw(...)` / `subscribe_raw(...)`: untyped raw API

## Notes

- Address must include a scheme, for example `http://127.0.0.1:4222`.
- Durable features require server-side store configuration.
- See docs.rs for complete API details.