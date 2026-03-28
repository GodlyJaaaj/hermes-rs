# hermes-client

Asynchronous Rust client to publish and consume events through the Hermes gRPC broker.

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
hermes-broker-client = "0.3"
hermes-broker-core   = "0.3"
serde  = { version = "1", features = ["derive"] }
tokio  = { version = "1", features = ["macros", "rt-multi-thread"] }
futures = "0.3"
```

## Quickstart — fire-and-forget pub/sub

> **Fanout rule:** subscribers only receive messages published **after** they
> connect. Always start the subscriber before publishing.

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

    // 1) Start the subscriber FIRST (fanout delivers only to active subscribers)
    let sub_client = client.clone();
    let handle = tokio::spawn(async move {
        let mut stream = sub_client.subscribe::<ChatMessage>(&[]).await.unwrap();
        while let Some(msg) = stream.next().await {
            println!("received: {:?}", msg.unwrap());
        }
    });

    // 2) Small delay so the subscription is registered on the broker
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 3) Now publish — the subscriber above will receive these
    for i in 0..5 {
        client
            .publish(&ChatMessage {
                user: "alice".into(),
                text: format!("Hello #{i}!"),
            })
            .await?;
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    handle.abort();
    Ok(())
}
```

## Queue groups — load-balanced consumption

When multiple subscribers join the same **queue group**, each message is
delivered to exactly **one** subscriber in the group (round-robin).

```rust,no_run
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct Task {
    id: u32,
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;

    // Spawn 3 workers in the same queue group
    for worker_id in 0..3 {
        let c = client.clone();
        tokio::spawn(async move {
            let mut stream = c.subscribe::<Task>(&["workers"]).await.unwrap();
            while let Some(Ok(task)) = stream.next().await {
                println!("Worker {worker_id} got task #{}: {}", task.id, task.payload);
            }
        });
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Each task goes to exactly one worker
    for i in 0..10 {
        client
            .publish(&Task { id: i, payload: format!("job-{i}") })
            .await?;
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}
```

## Custom subjects with `publish_on` / `subscribe_on`

When `#[derive(Event)]` auto-subjects are too static, use **custom subjects**
to route messages dynamically — for example, with an entity ID in the subject.

### Building a `Subject`

`Subject` uses a fluent builder:

```rust
use hermes_core::Subject;

// Concrete subject (no wildcards)
let subject = Subject::new().str("order").int(42).str("status");
// Display: "order.42.status"

// Pattern subject (with wildcards)
let pattern = Subject::new().str("order").any().str("status");
// Display: "order.*.status"

// Trailing wildcard — matches zero or more remaining segments
let catch_all = Subject::new().str("order").rest();
// Display: "order.>"
// Matches: "order", "order.42", "order.42.status", ...

// From a dot-separated string (integers are auto-parsed)
let s = Subject::from("order.42.status");
```

### Segment constructors

| Constructor | Produces | Display |
|---|---|---|
| `.str("x")` | `Segment::Str("x")` | `x` |
| `.int(42)` | `Segment::Int(42)` | `42` |
| `.any()` | `Segment::Any` | `*` |
| `.rest()` | `Segment::Rest` | `>` |
| `.segment(seg)` | any `Segment` value | — |

For standalone use: `Segment::s("x")`, `Segment::i(42)`, `Segment::any()`, `Segment::rest()`.

### `subscribe_on` — exemples pratiques

```rust,ignore
use hermes_client::HermesClient;
use hermes_core::{Event, Subject};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct JobLogs { line: String }

async fn examples(client: &HermesClient) -> Result<(), Box<dyn std::error::Error>> {
    // Écouter UN job spécifique (runtime)
    let job_id = "abc-123";
    let mut stream = client.subscribe_on::<JobLogs>(
        &Subject::new().str("job").str(job_id).str("logs"),  // concret, pas de wildcard
        &[],
    ).await?;

    // Écouter TOUS les jobs (wildcard)
    let mut stream = client.subscribe_on::<JobLogs>(
        &Subject::new().str("job").any().str("logs"),  // * = n'importe quel segment
        &[],
    ).await?;

    // Écouter les jobs qui commencent par "build" (runtime + wildcard)
    let prefix = "build";
    let mut stream = client.subscribe_on::<JobLogs>(
        &Subject::new().str("job").str(prefix).rest(),  // job.build.>
        &[],
    ).await?;

    Ok(())
}
```

## Event subjects with `derive(Event)`

By default, `#[derive(Event)]` generates a subject from `module_path + type_name`.

Override it explicitly with `#[event(subject = "...")]`:

```rust
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
#[event(subject = "orders.created")]
struct OrderCreated {
    order_id: String,
}
```

## Event groups — subscribe to multiple event types

`event_group!` merges several event types into a single stream, automatically
dispatched by subject:

```rust,no_run
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::{event_group, Event};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct OrderCreated { order_id: String }

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct OrderShipped { order_id: String, tracking_id: String }

event_group!(OrderEvents = [OrderCreated, OrderShipped]);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;

    let sub = client.clone();
    tokio::spawn(async move {
        let mut stream = sub.subscribe_group::<OrderEvents>(&[]).await.unwrap();
        while let Some(Ok(event)) = stream.next().await {
            match event {
                OrderEvents::OrderCreated(e)  => println!("created: {}", e.order_id),
                OrderEvents::OrderShipped(e) => println!("shipped: {}", e.tracking_id),
            }
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    client.publish(&OrderCreated { order_id: "ORD-1".into() }).await?;
    client.publish(&OrderShipped { order_id: "ORD-1".into(), tracking_id: "TRK-42".into() }).await?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}
```

## `BatchPublisher` — high-throughput publishing

`BatchPublisher` keeps a single gRPC client-stream open. All messages flow
through the same HTTP/2 stream, avoiding per-message round-trip overhead.

```rust,no_run
use hermes_client::HermesClient;
use hermes_core::{Event, Subject};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct Metric { name: String, value: f64 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;
    let batch = client.batch_publisher();

    // Send many events through the same stream
    for i in 0..10_000 {
        batch.send(&Metric { name: format!("cpu.{i}"), value: 0.42 }).await?;
    }

    // Or send raw payloads on a custom subject
    let subject = Subject::new().str("metrics").str("raw");
    batch.send_raw(&subject, b"raw payload".to_vec()).await?;

    // Close the stream and get the server ack
    let ack = batch.flush().await?;
    println!("server accepted {} messages", ack.accepted);
    Ok(())
}
```

## Durable subscriptions — at-least-once delivery

Durable mode persists messages server-side. Consumers **ack** or **nack**
each message; un-acked messages are automatically redelivered.

Requires the server to be configured with a store path (`HERMES_STORE_PATH`).

```rust,no_run
use hermes_client::HermesClient;
use hermes_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct Payment { id: String, amount: f64 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesClient::connect("http://127.0.0.1:4222").await?;

    // Durable publish — message is persisted before ack
    client.publish_durable(&Payment { id: "PAY-1".into(), amount: 99.99 }).await?;

    // Durable subscribe — messages are redelivered until acked
    let mut sub = client
        .subscribe_durable::<Payment>("payment-processor", &[], 10, 30)
        .await?;

    while let Some(msg) = sub.next().await {
        let msg = msg?;
        println!("processing payment: {:?}", msg.event);
        msg.ack().await?; // or msg.nack(true).await? to requeue
    }

    Ok(())
}
```

| Parameter | Description |
|---|---|
| `consumer_name` | Unique name for this consumer. Reusing the name resumes from the last acked offset. |
| `queue_groups` | Empty for fanout, or group names for load-balanced delivery. |
| `max_in_flight` | Maximum un-acked messages before the broker pauses delivery. |
| `ack_timeout_secs` | Seconds before un-acked messages are redelivered. |

## API reference

| Method | Description |
|---|---|
| `HermesClient::connect(uri)` | Connect to a Hermes broker |
| `publish(&event)` | Fire-and-forget publish (auto subject) |
| `publish_on(&event, &subject)` | Fire-and-forget publish on a custom subject |
| `publish_durable(&event)` | Durable publish (persisted before ack) |
| `subscribe::<E>(queue_groups)` | Typed subscription (auto subject) |
| `subscribe_on::<E>(&subject, queue_groups)` | Typed subscription on a custom subject (supports wildcards) |
| `subscribe_group::<G>(queue_groups)` | Event-group subscription (multiple types) |
| `subscribe_durable::<E>(name, groups, max, timeout)` | Durable subscription with ack/nack |
| `batch_publisher()` | High-throughput buffered publisher |
| `publish_raw(&subject, payload)` | Untyped raw publish |
| `subscribe_raw(&subject, groups)` | Untyped raw subscription |

## Notes

- Address must include a scheme, e.g. `http://127.0.0.1:4222`.
- Durable features require the server to be configured with a store path.
- See [docs.rs](https://docs.rs/hermes-broker-client) for complete API documentation.
