use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use scylla_broker_client::ScyllaBrokerClient;
use scylla_broker_core::{Event, event_group};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

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

// -- Helpers --

async fn start_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        scylla_broker_server::run(listener).await.unwrap();
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

    let publisher = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let subscriber = ScyllaBrokerClient::connect(&uri).await.unwrap();

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

    let publisher = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let sub1 = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let sub2 = ScyllaBrokerClient::connect(&uri).await.unwrap();

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

    let publisher = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let sub1 = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let sub2 = ScyllaBrokerClient::connect(&uri).await.unwrap();

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

    let publisher = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let subscriber = ScyllaBrokerClient::connect(&uri).await.unwrap();

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

    let publisher = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let subscriber = ScyllaBrokerClient::connect(&uri).await.unwrap();

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

    let client = ScyllaBrokerClient::connect(&uri).await.unwrap();
    let sub = ScyllaBrokerClient::connect(&uri).await.unwrap();

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
