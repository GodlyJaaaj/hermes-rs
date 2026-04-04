use std::net::SocketAddr;
use std::sync::Once;
use std::time::Duration;

use hermes_broker::router::{Router, RouterConfig};
use hermes_client::{Publisher, Subscriber};
use hermes_proto::broker_server::BrokerServer;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};

static INIT_TRACING: Once = Once::new();

fn init_test_tracing() {
    INIT_TRACING.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off")),
            )
            .with_test_writer()
            .try_init()
            .ok();
    });
}

/// Start a broker on a random port, return the channel to connect to.
async fn start_broker() -> Channel {
    init_test_tracing();
    let listener = TcpListener::bind("[::1]:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let (router, router_tx) = Router::new(RouterConfig::default(), 8192);
    tokio::spawn(router.run());

    let service = hermes_broker_server::grpc::BrokerService::new(router_tx);

    tokio::spawn(async move {
        Server::builder()
            .add_service(BrokerServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    Channel::from_shared(format!("http://[::1]:{}", addr.port()))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
async fn single_subscriber_exact_match() {
    let channel = start_broker().await;

    let mut subscriber = Subscriber::new(channel.clone()).await.unwrap();
    subscriber
        .subscribe("orders.eu.created", None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = Publisher::new(channel).await.unwrap();
    publisher
        .publish("orders.eu.created", &b"payload1"[..])
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .expect("timeout")
        .expect("stream closed");
    assert_eq!(msg.subject, "orders.eu.created");
    assert_eq!(msg.payload, b"payload1");
}

#[tokio::test]
async fn wildcard_star_match() {
    let channel = start_broker().await;

    let mut subscriber = Subscriber::new(channel.clone()).await.unwrap();
    subscriber
        .subscribe("orders.*.created", None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = Publisher::new(channel).await.unwrap();
    publisher
        .publish("orders.eu.created", &b"eu"[..])
        .await
        .unwrap();
    publisher
        .publish("orders.us.created", &b"us"[..])
        .await
        .unwrap();

    let msg1 = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let msg2 = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg1.payload, b"eu");
    assert_eq!(msg2.payload, b"us");
}

#[tokio::test]
async fn tail_match() {
    let channel = start_broker().await;

    let mut subscriber = Subscriber::new(channel.clone()).await.unwrap();
    subscriber.subscribe("orders.>", None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = Publisher::new(channel).await.unwrap();
    publisher
        .publish("orders.eu.created", &b"deep"[..])
        .await
        .unwrap();
    publisher
        .publish("orders.us", &b"shallow"[..])
        .await
        .unwrap();

    let msg1 = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    let msg2 = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg1.payload, b"deep");
    assert_eq!(msg2.payload, b"shallow");
}

#[tokio::test]
async fn fanout_multiple_subscribers() {
    let channel = start_broker().await;

    let mut sub1 = Subscriber::new(channel.clone()).await.unwrap();
    let mut sub2 = Subscriber::new(channel.clone()).await.unwrap();
    let mut sub3 = Subscriber::new(channel.clone()).await.unwrap();

    sub1.subscribe("events.fanout", None).await.unwrap();
    sub2.subscribe("events.fanout", None).await.unwrap();
    sub3.subscribe("events.fanout", None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = Publisher::new(channel).await.unwrap();
    publisher
        .publish("events.fanout", &b"broadcast"[..])
        .await
        .unwrap();

    for sub in [&mut sub1, &mut sub2, &mut sub3] {
        let msg = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload, b"broadcast");
    }
}

#[tokio::test]
async fn queue_group_round_robin() {
    let channel = start_broker().await;

    let mut sub1 = Subscriber::new(channel.clone()).await.unwrap();
    let mut sub2 = Subscriber::new(channel.clone()).await.unwrap();

    sub1.subscribe("jobs.process", Some("workers".into()))
        .await
        .unwrap();
    sub2.subscribe("jobs.process", Some("workers".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = Publisher::new(channel).await.unwrap();
    for i in 0..4 {
        publisher
            .publish("jobs.process", format!("msg{i}").into_bytes())
            .await
            .unwrap();
    }

    let mut all_payloads = Vec::new();
    for sub in [&mut sub1, &mut sub2] {
        while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await
        {
            all_payloads.push(String::from_utf8(msg.payload).unwrap());
        }
    }

    all_payloads.sort();
    assert_eq!(all_payloads, vec!["msg0", "msg1", "msg2", "msg3"]);
}

#[tokio::test]
async fn disconnect_cleans_up_subscriber() {
    let channel = start_broker().await;

    // Subscribe, then drop the subscriber to trigger disconnect.
    {
        let sub = Subscriber::new(channel.clone()).await.unwrap();
        sub.subscribe("cleanup.test", None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // sub dropped here → gRPC stream closes → router cleans up
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // New subscriber on same subject should work cleanly.
    let mut sub2 = Subscriber::new(channel.clone()).await.unwrap();
    sub2.subscribe("cleanup.test", None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let publisher = Publisher::new(channel).await.unwrap();
    publisher
        .publish("cleanup.test", &b"after-cleanup"[..])
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.payload, b"after-cleanup");
}
