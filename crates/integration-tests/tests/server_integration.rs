use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::{Event, Segment, Subject, event_group};
use hermes_server::config::ServerConfig;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// -- Test event types --

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
struct UserCreated {
    user_id: String,
    email: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
struct UserDeleted {
    user_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
enum OrderEvent {
    Placed { order_id: String, total: f64 },
    Shipped { order_id: String },
}

event_group!(UserEvents = [UserCreated, UserDeleted]);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
struct JobLog {
    job_id: String,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Event)]
#[event(subject = "notifications.alert")]
struct Alert {
    severity: String,
    message: String,
}

// -- Helpers --

async fn start_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        hermes_server::run(listener).await.unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn addr_to_uri(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

// -- Tests --

#[tokio::test]
async fn test_publish_subscribe_single_event() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let subscriber = HermesClient::connect(&uri).await.unwrap();

    let mut stream = subscriber.subscribe::<UserCreated>(&[]).await.unwrap();

    // Small delay to ensure subscription is registered
    tokio::time::sleep(Duration::from_millis(20)).await;

    let event = UserCreated {
        user_id: "u1".into(),
        email: "alice@test.com".into(),
    };
    publisher.publish(&event).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout waiting for event")
        .expect("stream ended")
        .expect("decode error");

    assert_eq!(received, event);
}

#[tokio::test]
async fn test_fanout_multiple_subscribers() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let sub1 = HermesClient::connect(&uri).await.unwrap();
    let sub2 = HermesClient::connect(&uri).await.unwrap();

    let mut stream1 = sub1.subscribe::<UserCreated>(&[]).await.unwrap();
    let mut stream2 = sub2.subscribe::<UserCreated>(&[]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let event = UserCreated {
        user_id: "u2".into(),
        email: "bob@test.com".into(),
    };
    publisher.publish(&event).await.unwrap();

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

    assert_eq!(r1, event);
    assert_eq!(r2, event);
}

#[tokio::test]
async fn test_queue_group() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let sub1 = HermesClient::connect(&uri).await.unwrap();
    let sub2 = HermesClient::connect(&uri).await.unwrap();

    let mut stream1 = sub1.subscribe::<UserCreated>(&["workers"]).await.unwrap();
    let mut stream2 = sub2.subscribe::<UserCreated>(&["workers"]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let event = UserCreated {
        user_id: "u3".into(),
        email: "charlie@test.com".into(),
    };
    publisher.publish(&event).await.unwrap();

    // Exactly one should receive it
    let r1 = tokio::time::timeout(Duration::from_millis(500), stream1.next()).await;
    let r2 = tokio::time::timeout(Duration::from_millis(500), stream2.next()).await;

    let got1 = r1.is_ok();
    let got2 = r2.is_ok();
    assert!(
        got1 ^ got2,
        "exactly one subscriber should receive the message, got1={got1}, got2={got2}"
    );
}

#[tokio::test]
async fn test_enum_event() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let subscriber = HermesClient::connect(&uri).await.unwrap();

    let mut stream = subscriber.subscribe::<OrderEvent>(&[]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let event = OrderEvent::Placed {
        order_id: "o1".into(),
        total: 99.99,
    };
    publisher.publish(&event).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(received, event);
}

#[tokio::test]
async fn test_event_group() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let subscriber = HermesClient::connect(&uri).await.unwrap();

    let mut stream = subscriber.subscribe_group::<UserEvents>(&[]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish a UserCreated
    publisher
        .publish(&UserCreated {
            user_id: "u4".into(),
            email: "dave@test.com".into(),
        })
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    match received {
        UserEvents::UserCreated(uc) => {
            assert_eq!(uc.user_id, "u4");
            assert_eq!(uc.email, "dave@test.com");
        }
        _ => panic!("expected UserCreated variant"),
    }

    // Publish a UserDeleted
    publisher
        .publish(&UserDeleted {
            user_id: "u4".into(),
        })
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    match received {
        UserEvents::UserDeleted(ud) => {
            assert_eq!(ud.user_id, "u4");
        }
        _ => panic!("expected UserDeleted variant"),
    }
}

#[tokio::test]
async fn test_batch_publisher() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let client = HermesClient::connect(&uri).await.unwrap();
    let sub = HermesClient::connect(&uri).await.unwrap();

    let mut stream = sub.subscribe::<UserCreated>(&[]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let batch = client.batch_publisher();
    for i in 0..5 {
        batch
            .send(&UserCreated {
                user_id: format!("u_{i}"),
                email: format!("user{i}@test.com"),
            })
            .await
            .unwrap();
    }
    let ack = batch.flush().await.unwrap();
    assert_eq!(ack.accepted, 5);

    // Receive all 5
    for i in 0..5 {
        let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(received.user_id, format!("u_{i}"));
    }
}

#[tokio::test]
async fn test_custom_subject_fanout_two_clients() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    // Three independent connections: one publisher, two subscribers
    let publisher = HermesClient::connect(&uri).await.unwrap();
    let subscriber_a = HermesClient::connect(&uri).await.unwrap();
    let subscriber_b = HermesClient::connect(&uri).await.unwrap();

    // Both subscribe to Alert (custom subject "notifications.alert")
    let mut stream_a = subscriber_a.subscribe::<Alert>(&[]).await.unwrap();
    let mut stream_b = subscriber_b.subscribe::<Alert>(&[]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish 3 alerts
    let alerts: Vec<Alert> = (0..3)
        .map(|i| Alert {
            severity: if i == 0 {
                "critical".into()
            } else {
                "warning".into()
            },
            message: format!("alert #{i}"),
        })
        .collect();

    for alert in &alerts {
        publisher.publish(alert).await.unwrap();
    }

    // Both subscribers should receive all 3 (fanout)
    for (i, expected) in alerts.iter().enumerate() {
        let a = tokio::time::timeout(Duration::from_secs(2), stream_a.next())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for alert #{i} on subscriber A"))
            .expect("stream A ended")
            .expect("decode error on A");

        let b = tokio::time::timeout(Duration::from_secs(2), stream_b.next())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for alert #{i} on subscriber B"))
            .expect("stream B ended")
            .expect("decode error on B");

        assert_eq!(&a, expected, "subscriber A mismatch on alert #{i}");
        assert_eq!(&b, expected, "subscriber B mismatch on alert #{i}");
    }
}

// -- publish_on / subscribe_on tests --

#[tokio::test]
async fn test_publish_on_subscribe_on_with_wildcard() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let subscriber = HermesClient::connect(&uri).await.unwrap();

    // Subscribe with wildcard: job.*.logs
    let pattern = Subject::new().str("job").any().str("logs");
    let mut stream = subscriber
        .subscribe_on::<JobLog>(&pattern, &[])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish on concrete subjects — all should match the wildcard
    for id in ["build", "deploy", "test"] {
        let log = JobLog {
            job_id: id.into(),
            message: format!("{id} completed"),
        };
        let subject = Subject::new().str("job").str(id).str("logs");
        publisher.publish_on(&log, &subject).await.unwrap();
    }

    // Should receive all 3 messages, typed
    for expected_id in ["build", "deploy", "test"] {
        let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for job {expected_id}"))
            .expect("stream ended")
            .expect("decode error");
        assert_eq!(received.job_id, expected_id);
        assert_eq!(received.message, format!("{expected_id} completed"));
    }
}

#[tokio::test]
async fn test_publish_on_subscribe_on_no_wildcard() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let subscriber = HermesClient::connect(&uri).await.unwrap();

    // Subscribe on a specific concrete subject (no wildcard)
    let subject = Subject::new().str("job").str("build").str("logs");
    let mut stream = subscriber
        .subscribe_on::<JobLog>(&subject, &[])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish on matching subject
    let log = JobLog {
        job_id: "build".into(),
        message: "done".into(),
    };
    publisher.publish_on(&log, &subject).await.unwrap();

    // Publish on non-matching subject — should NOT be received
    let other = Subject::new().str("job").str("deploy").str("logs");
    publisher
        .publish_on(
            &JobLog {
                job_id: "deploy".into(),
                message: "done".into(),
            },
            &other,
        )
        .await
        .unwrap();

    // Should receive only the matching one
    let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(received.job_id, "build");

    // No second message
    let timeout = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(timeout.is_err(), "should not receive non-matching message");
}

#[tokio::test]
async fn test_subject_template_publish_subscribe() {
    let addr = start_broker().await;
    let uri = addr_to_uri(addr);

    let publisher = HermesClient::connect(&uri).await.unwrap();
    let subscriber = HermesClient::connect(&uri).await.unwrap();

    // Subscribe with wildcard: job.*.logs
    let pattern = Subject::new().str("job").any().str("logs");
    let mut stream = subscriber
        .subscribe_on::<JobLog>(&pattern, &[])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish on concrete subjects
    for id in ["build", "deploy", "test"] {
        let log = JobLog {
            job_id: id.into(),
            message: format!("{id} ok"),
        };
        let subject = Subject::new().str("job").str(id).str("logs");
        publisher.publish_on(&log, &subject).await.unwrap();
    }

    // All 3 match the pattern
    for expected_id in ["build", "deploy", "test"] {
        let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for {expected_id}"))
            .expect("stream ended")
            .expect("decode error");
        assert_eq!(received.job_id, expected_id);
    }
}

// -- Durable / persistence helpers --

/// Start a broker with durable store enabled and fast redelivery (1s interval).
async fn start_durable_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let tmp_dir = tempfile::tempdir().unwrap();
    let store_path = tmp_dir.path().join("hermes-test.redb");
    // Leak the tempdir so it lives as long as the process.
    std::mem::forget(tmp_dir);

    let cfg = ServerConfig {
        store_path: Some(store_path),
        redelivery_interval_secs: 1,
        default_ack_timeout_secs: 1,
        max_delivery_attempts: 5,
        ..ServerConfig::default()
    };

    tokio::spawn(async move {
        hermes_server::run_with_config(listener, cfg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

// -- Durable persistence tests --

/// Publish durable messages, subscriber connects but does NOT ack,
/// subscriber disconnects, then reconnects with the same consumer_name.
/// The unacked messages should be redelivered.
#[tokio::test]
async fn test_durable_persistence_redelivery_on_reconnect() {
    let addr = start_durable_broker().await;
    let uri = addr_to_uri(addr);

    // Publish 3 durable messages.
    let publisher = HermesClient::connect(&uri).await.unwrap();
    for i in 0..3 {
        publisher
            .publish_durable(&UserCreated {
                user_id: format!("persist_{i}"),
                email: format!("user{i}@persist.com"),
            })
            .await
            .unwrap();
    }

    // First subscriber: connect, receive messages, do NOT ack, then disconnect.
    {
        let sub_client = HermesClient::connect(&uri).await.unwrap();
        let mut durable = sub_client
            .subscribe_durable::<UserCreated>("test-consumer-1", &[], 10, 1)
            .await
            .unwrap();

        let mut received = Vec::new();
        for _ in 0..3 {
            let msg = tokio::time::timeout(Duration::from_secs(3), durable.next())
                .await
                .expect("timeout waiting for durable message")
                .expect("stream ended");
            let msg = msg.expect("decode error");
            received.push(msg.event.user_id.clone());
            // Intentionally NOT calling msg.ack()
        }
        assert_eq!(received.len(), 3);
        // durable + sub_client dropped here → subscriber disconnects
    }

    // Wait for ack timeout + redelivery loop to fire.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Second subscriber: reconnect with the SAME consumer name.
    let sub_client2 = HermesClient::connect(&uri).await.unwrap();
    let mut durable2 = sub_client2
        .subscribe_durable::<UserCreated>("test-consumer-1", &[], 10, 30)
        .await
        .unwrap();

    // Should receive the 3 messages again (redelivered).
    let mut redelivered = Vec::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), durable2.next())
            .await
            .expect("timeout: message was not redelivered")
            .expect("stream ended");
        let msg = msg.expect("decode error");
        redelivered.push(msg.event.user_id.clone());
        msg.ack().await.unwrap();
    }

    redelivered.sort();
    assert_eq!(redelivered, vec!["persist_0", "persist_1", "persist_2"]);
}

/// Publish durable, subscriber acks all, disconnects, reconnects
/// → should receive nothing (messages were acked).
#[tokio::test]
async fn test_durable_acked_messages_not_redelivered() {
    let addr = start_durable_broker().await;
    let uri = addr_to_uri(addr);

    // Subscribe FIRST so messages are delivered in real-time.
    let sub_client = HermesClient::connect(&uri).await.unwrap();
    let mut durable = sub_client
        .subscribe_durable::<UserCreated>("test-consumer-ack", &[], 10, 30)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = HermesClient::connect(&uri).await.unwrap();
    for i in 0..3 {
        publisher
            .publish_durable(&UserCreated {
                user_id: format!("acked_{i}"),
                email: format!("user{i}@acked.com"),
            })
            .await
            .unwrap();
    }

    // Receive + ACK all.
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(3), durable.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("decode error");
        msg.ack().await.unwrap();
    }

    // Wait for acks to be fully processed server-side before disconnecting.
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(durable);
    drop(sub_client);

    // Wait long enough for the server to fully process the disconnect.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Reconnect → should get nothing.
    let sub_client2 = HermesClient::connect(&uri).await.unwrap();
    let mut durable2 = sub_client2
        .subscribe_durable::<UserCreated>("test-consumer-ack", &[], 10, 30)
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), durable2.next()).await;
    assert!(
        result.is_err(),
        "should NOT receive any message after all were acked"
    );
}

/// Test: publish durable BEFORE subscribe → message comes from catch_up_durable.
#[tokio::test]
async fn test_durable_catchup_ack() {
    let addr = start_durable_broker().await;
    let uri = addr_to_uri(addr);

    // Publish FIRST (no consumer registered yet → message stays Pending).
    let publisher = HermesClient::connect(&uri).await.unwrap();
    publisher
        .publish_durable(&UserCreated {
            user_id: "catchup".into(),
            email: "catchup@test.com".into(),
        })
        .await
        .unwrap();

    // Subscribe AFTER → catch_up_durable delivers the pending message.
    let sub_client = HermesClient::connect(&uri).await.unwrap();
    let mut durable = sub_client
        .subscribe_durable::<UserCreated>("catchup-consumer", &[], 10, 30)
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(3), durable.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("decode error");
    assert_eq!(msg.event.user_id, "catchup");
    msg.ack().await.unwrap();
}

/// Minimal test: durable pub/sub with ack to verify the basic flow works.
#[tokio::test]
async fn test_durable_basic_ack() {
    let addr = start_durable_broker().await;
    let uri = addr_to_uri(addr);

    let client = HermesClient::connect(&uri).await.unwrap();
    let mut durable = client
        .subscribe_durable::<UserCreated>("basic-ack-consumer", &[], 10, 30)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = HermesClient::connect(&uri).await.unwrap();
    publisher
        .publish_durable(&UserCreated {
            user_id: "basic".into(),
            email: "basic@test.com".into(),
        })
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(3), durable.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("decode error");
    assert_eq!(msg.event.user_id, "basic");
    msg.ack().await.unwrap();
}

/// Nack with requeue=true → message goes back to Pending.
/// On reconnect the consumer gets it again via catch_up_durable.
#[tokio::test]
async fn test_durable_nack_requeue() {
    let addr = start_durable_broker().await;
    let uri = addr_to_uri(addr);

    // Subscribe FIRST so the message is delivered in real-time.
    let sub_client = HermesClient::connect(&uri).await.unwrap();
    let mut durable = sub_client
        .subscribe_durable::<UserCreated>("test-consumer-nack", &[], 10, 30)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = HermesClient::connect(&uri).await.unwrap();
    publisher
        .publish_durable(&UserCreated {
            user_id: "nack_me".into(),
            email: "nack@test.com".into(),
        })
        .await
        .unwrap();

    // Receive and NACK with requeue.
    let msg = tokio::time::timeout(Duration::from_secs(3), durable.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("decode error");
    assert_eq!(msg.event.user_id, "nack_me");
    msg.nack(true).await.unwrap();

    // Wait for nack to be processed, then disconnect.
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(durable);
    drop(sub_client);

    // Wait long enough for the server to fully process the disconnect
    // (unsubscribe_durable) before reconnecting with the same consumer name.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Reconnect → catch_up_durable should redeliver the nacked message.
    let sub_client2 = HermesClient::connect(&uri).await.unwrap();
    let mut durable2 = sub_client2
        .subscribe_durable::<UserCreated>("test-consumer-nack", &[], 10, 30)
        .await
        .unwrap();

    let msg2 = tokio::time::timeout(Duration::from_secs(5), durable2.next())
        .await
        .expect("timeout: nacked message was not redelivered on reconnect")
        .expect("stream ended")
        .expect("decode error");
    assert_eq!(msg2.event.user_id, "nack_me");
    msg2.ack().await.unwrap();
}

// -- Server restart persistence test --

/// A running durable broker that can be gracefully shut down.
struct DurableBrokerHandle {
    addr: SocketAddr,
    store_path: PathBuf,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl DurableBrokerHandle {
    fn uri(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Gracefully shut down the server and wait for it to fully stop
    /// (including releasing the redb file lock).
    async fn shutdown(self) {
        self.cancel.cancel();
        // Wait for the server task to finish — this ensures the store is released.
        let _ = self.task.await;
    }
}

/// Start a durable broker with graceful shutdown support.
async fn start_durable_broker_with_shutdown() -> DurableBrokerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let tmp_dir = tempfile::tempdir().unwrap();
    let store_path = tmp_dir.path().join("hermes-restart.redb");
    std::mem::forget(tmp_dir);

    start_broker_on(addr, listener, store_path).await
}

/// Restart a broker on a new port, reusing the same store file.
async fn restart_broker_with_store(store_path: PathBuf) -> DurableBrokerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    start_broker_on(addr, listener, store_path).await
}

async fn start_broker_on(
    addr: SocketAddr,
    listener: TcpListener,
    store_path: PathBuf,
) -> DurableBrokerHandle {
    let cfg = ServerConfig {
        store_path: Some(store_path.clone()),
        redelivery_interval_secs: 1,
        default_ack_timeout_secs: 1,
        max_delivery_attempts: 5,
        ..ServerConfig::default()
    };

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    let task = tokio::spawn(async move {
        hermes_server::run_with_shutdown(listener, cfg, cancel_clone.cancelled())
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    DurableBrokerHandle {
        addr,
        store_path,
        cancel,
        task,
    }
}

/// Full server restart test:
/// 1. Start server, publish durable messages, subscriber receives but does NOT ack
/// 2. Shut down the server (graceful shutdown)
/// 3. Restart with the SAME store file on a new port
/// 4. Subscriber reconnects → should receive all unacked messages
#[tokio::test]
async fn test_durable_persistence_survives_server_restart() {
    // Phase 1: Start server, publish, receive without ack.
    let handle = start_durable_broker_with_shutdown().await;
    let store_path = handle.store_path.clone();
    let uri1 = handle.uri();

    // Subscribe first so messages are delivered in real-time.
    // Use ack_timeout=1s so messages expire quickly after the server restarts.
    let sub_client = HermesClient::connect(&uri1).await.unwrap();
    let mut durable = sub_client
        .subscribe_durable::<UserCreated>("restart-consumer", &[], 10, 1)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = HermesClient::connect(&uri1).await.unwrap();
    for i in 0..3 {
        publisher
            .publish_durable(&UserCreated {
                user_id: format!("restart_{i}"),
                email: format!("restart{i}@test.com"),
            })
            .await
            .unwrap();
    }

    // Receive all 3 messages but do NOT ack.
    let mut received = Vec::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(3), durable.next())
            .await
            .expect("timeout waiting for message")
            .expect("stream ended")
            .expect("decode error");
        received.push(msg.event.user_id.clone());
        // Intentionally NOT acking.
    }
    assert_eq!(received.len(), 3);

    // Phase 2: Drop clients, then gracefully shut down the server.
    // Drop clients first to close gRPC connections cleanly.
    drop(durable);
    drop(sub_client);
    drop(publisher);
    // Shut down and WAIT for the server to fully stop (releases redb lock).
    handle.shutdown().await;

    // Phase 3: Restart with the SAME store file on a new port.
    let handle2 = restart_broker_with_store(store_path).await;
    let uri2 = handle2.uri();

    // Phase 4: Reconnect and verify messages are redelivered from disk.
    let sub_client2 = HermesClient::connect(&uri2).await.unwrap();
    let mut durable2 = sub_client2
        .subscribe_durable::<UserCreated>("restart-consumer", &[], 10, 30)
        .await
        .unwrap();

    let mut redelivered = Vec::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), durable2.next())
            .await
            .expect("timeout: message was NOT redelivered after server restart")
            .expect("stream ended")
            .expect("decode error");
        redelivered.push(msg.event.user_id.clone());
        msg.ack().await.unwrap();
    }

    redelivered.sort();
    assert_eq!(
        redelivered,
        vec!["restart_0", "restart_1", "restart_2"],
        "all 3 messages should survive server restart"
    );
}
