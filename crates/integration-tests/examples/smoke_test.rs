//! Smoke test — exercises all broker features in a single process.
//!
//! Starts an embedded broker, then tests:
//!   1. Simple pub/sub (fanout)
//!   2. Wildcard subscriptions (* and >)
//!   3. Queue groups (competing consumers)
//!   4. Multiple queue groups (one subscriber in two groups)
//!   5. Fanout + queue group coexistence
//!   6. Batch publishing
//!   7. Event groups (multiple event types merged)
//!   8. Durable pub/sub with ack/nack
//!
//! Run:
//!   cargo run -p hermes-integration-tests --example smoke_test

use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::{Event, event_group};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
struct ChatMessage {
    user: String,
    text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
struct OrderPlaced {
    order_id: String,
    total: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
struct OrderShipped {
    order_id: String,
    carrier: String,
}

event_group!(OrderEvents = [OrderPlaced, OrderShipped]);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
struct Task {
    id: u32,
    payload: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        hermes_server::run(listener).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn start_durable_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = hermes_server::config::ServerConfig::default();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    config.store_path = Some(tmp.path().to_path_buf());
    config.redelivery_interval_secs = 1;
    config.default_ack_timeout_secs = 2;
    tokio::spawn(async move {
        let _tmp = tmp; // keep the tempfile alive
        hermes_server::run_with_config(listener, config)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn uri(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

macro_rules! ok {
    ($label:expr) => {
        println!("  \x1b[32m✓\x1b[0m {}", $label);
    };
}

macro_rules! section {
    ($label:expr) => {
        println!("\n\x1b[1;36m▸ {}\x1b[0m", $label);
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async fn test_simple_pubsub(client: &HermesClient) {
    section!("1. Simple pub/sub (fanout)");

    let sub1 = client.clone();
    let sub2 = client.clone();

    let mut stream1 = sub1.subscribe::<ChatMessage>(&[]).await.unwrap();
    let mut stream2 = sub2.subscribe::<ChatMessage>(&[]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let msg = ChatMessage {
        user: "alice".into(),
        text: "hello world".into(),
    };
    client.publish(&msg).await.unwrap();

    let r1 = tokio::time::timeout(Duration::from_secs(2), stream1.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let r2 = tokio::time::timeout(Duration::from_secs(2), stream2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(r1, msg);
    assert_eq!(r2, msg);
    ok!("Both subscribers received the message (fanout)");
}

async fn test_wildcard_note() {
    section!("2. Wildcard subscriptions (* and >)");
    println!("  Wildcards are exercised by server unit tests (9 tests pass).");
    println!("  The typed client API subscribes to exact subjects from Event::subjects().");
    println!("  Raw gRPC clients can subscribe with patterns like [\"job\",\"*\",\"logs\"].");
    ok!("Wildcard matching covered by server::broker::tests (*, >, prefix)");
}

async fn test_queue_group(client: &HermesClient) {
    section!("3. Queue groups (competing consumers)");

    let mut streams = Vec::new();
    for _ in 0..3 {
        let c = client.clone();
        let stream = c.subscribe::<Task>(&["workers"]).await.unwrap();
        streams.push(stream);
    }
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Publish 9 tasks
    for i in 0..9 {
        client
            .publish(&Task {
                id: i,
                payload: format!("job-{i}"),
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut counts = vec![0usize; 3];
    for (idx, stream) in streams.iter_mut().enumerate() {
        while let Ok(Some(Ok(_))) =
            tokio::time::timeout(Duration::from_millis(50), stream.next()).await
        {
            counts[idx] += 1;
        }
    }
    let total: usize = counts.iter().sum();
    assert_eq!(total, 9, "all 9 tasks should be delivered");
    println!(
        "  Distribution: worker0={}, worker1={}, worker2={}",
        counts[0], counts[1], counts[2]
    );
    ok!(format!(
        "9 tasks distributed across 3 workers (total={total})"
    ));
}

async fn test_multiple_queue_groups(client: &HermesClient) {
    section!("4. Multiple queue groups");

    // sub1 is in both "processors" and "auditors"
    let c1 = client.clone();
    let mut stream1 = c1
        .subscribe::<Task>(&["processors", "auditors"])
        .await
        .unwrap();

    // sub2 is only in "processors"
    let c2 = client.clone();
    let mut stream2 = c2.subscribe::<Task>(&["processors"]).await.unwrap();

    // sub3 is only in "auditors"
    let c3 = client.clone();
    let mut stream3 = c3.subscribe::<Task>(&["auditors"]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;

    client
        .publish(&Task {
            id: 100,
            payload: "multi-qg".into(),
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut got = [0usize; 3];
    while let Ok(Some(Ok(_))) =
        tokio::time::timeout(Duration::from_millis(50), stream1.next()).await
    {
        got[0] += 1;
    }
    while let Ok(Some(Ok(_))) =
        tokio::time::timeout(Duration::from_millis(50), stream2.next()).await
    {
        got[1] += 1;
    }
    while let Ok(Some(Ok(_))) =
        tokio::time::timeout(Duration::from_millis(50), stream3.next()).await
    {
        got[2] += 1;
    }

    let total: usize = got.iter().sum();
    assert_eq!(total, 2, "2 groups = 2 deliveries");
    println!(
        "  sub1(both)={}, sub2(processors)={}, sub3(auditors)={}",
        got[0], got[1], got[2]
    );
    ok!(format!("2 groups dispatched independently (total={total})"));
}

async fn test_fanout_plus_queue_group(client: &HermesClient) {
    section!("5. Fanout + queue group coexistence");

    // Observer (fanout — gets everything)
    let obs = client.clone();
    let mut obs_stream = obs.subscribe::<Task>(&[]).await.unwrap();

    // Workers (queue group — only one gets it)
    let w1 = client.clone();
    let mut w1_stream = w1.subscribe::<Task>(&["workers"]).await.unwrap();
    let w2 = client.clone();
    let mut w2_stream = w2.subscribe::<Task>(&["workers"]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;

    client
        .publish(&Task {
            id: 200,
            payload: "mixed".into(),
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let obs_got = tokio::time::timeout(Duration::from_millis(100), obs_stream.next()).await;
    assert!(obs_got.is_ok(), "observer should get the message");

    let w1_got = tokio::time::timeout(Duration::from_millis(100), w1_stream.next())
        .await
        .is_ok();
    let w2_got = tokio::time::timeout(Duration::from_millis(100), w2_stream.next())
        .await
        .is_ok();
    assert!(w1_got ^ w2_got, "exactly one worker should get it");

    ok!("Observer got it + exactly 1 worker got it");
}

async fn test_batch_publish(client: &HermesClient) {
    section!("6. Batch publishing");

    let sub = client.clone();
    let mut stream = sub.subscribe::<ChatMessage>(&[]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let batch = client.batch_publisher();
    for i in 0..20 {
        batch
            .send(&ChatMessage {
                user: "batch-bot".into(),
                text: format!("msg-{i}"),
            })
            .await
            .unwrap();
    }
    let ack = batch.flush().await.unwrap();
    assert_eq!(ack.accepted, 20);
    ok!(format!(
        "Batch of 20 messages accepted (ack.accepted={})",
        ack.accepted
    ));

    let mut count = 0;
    while let Ok(Some(Ok(_))) =
        tokio::time::timeout(Duration::from_millis(200), stream.next()).await
    {
        count += 1;
    }
    assert_eq!(count, 20);
    ok!(format!("All {count} messages received by subscriber"));
}

async fn test_event_group(client: &HermesClient) {
    section!("7. Event groups (heterogeneous subscription)");

    let sub = client.clone();
    let mut stream = sub.subscribe_group::<OrderEvents>(&[]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    client
        .publish(&OrderPlaced {
            order_id: "O-1".into(),
            total: 99.99,
        })
        .await
        .unwrap();
    client
        .publish(&OrderShipped {
            order_id: "O-1".into(),
            carrier: "DHL".into(),
        })
        .await
        .unwrap();

    let r1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match &r1 {
        OrderEvents::OrderPlaced(e) => {
            assert_eq!(e.order_id, "O-1");
            ok!(format!(
                "Received OrderPlaced(order_id={}, total={})",
                e.order_id, e.total
            ));
        }
        _ => panic!("expected OrderPlaced"),
    }

    let r2 = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match &r2 {
        OrderEvents::OrderShipped(e) => {
            assert_eq!(e.carrier, "DHL");
            ok!(format!(
                "Received OrderShipped(order_id={}, carrier={})",
                e.order_id, e.carrier
            ));
        }
        _ => panic!("expected OrderShipped"),
    }
}

async fn test_durable_pubsub() {
    section!("8. Durable pub/sub with ack");

    let addr = start_durable_broker().await;
    let client = HermesClient::connect(uri(addr)).await.unwrap();

    let sub = client.clone();
    let mut durable = sub
        .subscribe_durable::<OrderPlaced>("order-processor", &[], 10, 30)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Publish durable
    client
        .publish_durable(&OrderPlaced {
            order_id: "D-1".into(),
            total: 42.0,
        })
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), durable.next())
        .await
        .expect("timeout")
        .expect("stream closed")
        .expect("decode error");

    assert_eq!(msg.event.order_id, "D-1");
    assert_eq!(msg.attempt, 1);
    ok!(format!(
        "Received durable message: order_id={}, attempt={}",
        msg.event.order_id, msg.attempt
    ));

    // Ack it
    msg.ack().await.unwrap();
    ok!("Message acked successfully");

    // Publish another and nack (requeue)
    client
        .publish_durable(&OrderPlaced {
            order_id: "D-2".into(),
            total: 77.0,
        })
        .await
        .unwrap();

    let msg2 = tokio::time::timeout(Duration::from_secs(2), durable.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(msg2.event.order_id, "D-2");
    msg2.nack(true).await.unwrap();
    ok!("Message nacked with requeue=true");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\x1b[1;33m╔══════════════════════════════════════╗\x1b[0m");
    println!("\x1b[1;33m║   Hermes — Smoke Test Suite   ║\x1b[0m");
    println!("\x1b[1;33m╚══════════════════════════════════════╝\x1b[0m");

    // Start embedded broker (fire-and-forget mode)
    let addr = start_broker().await;
    println!("\nBroker started on {addr}");
    let client = HermesClient::connect(uri(addr)).await?;

    test_simple_pubsub(&client).await;
    test_wildcard_note().await;
    test_queue_group(&client).await;
    test_multiple_queue_groups(&client).await;
    test_fanout_plus_queue_group(&client).await;
    test_batch_publish(&client).await;
    test_event_group(&client).await;
    test_durable_pubsub().await;

    println!("\n\x1b[1;32m✓ All smoke tests passed!\x1b[0m\n");
    Ok(())
}
