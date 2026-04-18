//! End-to-end gRPC benchmark: client -> tonic server -> router -> broadcast
//! -> tonic subscribe stream -> client. Exercises the wire path the
//! router-only benches (`crates/core/benches/throughput.rs`) skip, so the
//! win from the subscribe-stream rewrite (no per-sub forwarder task, no
//! extra mpsc hop, Bytes-backed payload) is observable here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hermes_broker::router::{Router, RouterConfig};
use hermes_broker_server::grpc::BrokerService;
use hermes_client::{Publisher, Subscriber};
use hermes_proto::broker_server::BrokerServer;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Server};

fn payload() -> Bytes {
    Bytes::from(vec![0xABu8; 128])
}

async fn start_broker() -> Channel {
    let listener = TcpListener::bind("[::1]:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let (router, router_tx) = Router::new(RouterConfig::default(), 8192);
    tokio::spawn(router.run());

    let service = BrokerService::new(router_tx);
    tokio::spawn(async move {
        Server::builder()
            .add_service(BrokerServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .ok();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    Channel::from_shared(format!("http://[::1]:{}", addr.port()))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// N gRPC subscribers, 1 publisher. Per iter: publish 1 message, wait until
/// every subscriber received it.
fn bench_grpc_fanout_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("grpc_fanout_e2e");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));

    for &n_subs in &[1usize, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("subscribers", n_subs),
            &n_subs,
            |b, &n_subs| {
                let (publisher, subs) = rt.block_on(async {
                    let channel = start_broker().await;

                    let publisher = Arc::new(Publisher::new(channel.clone()));
                    let mut subs = Vec::with_capacity(n_subs);
                    for _ in 0..n_subs {
                        let s = Subscriber::new(channel.clone()).await.unwrap();
                        s.subscribe("bench.grpc.fanout", None).await.unwrap();
                        subs.push(s);
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                    (publisher, Arc::new(Mutex::new(subs)))
                });

                b.to_async(&rt).iter(|| {
                    let publisher = Arc::clone(&publisher);
                    let subs = Arc::clone(&subs);
                    async move {
                        publisher
                            .publish("bench.grpc.fanout", payload())
                            .await
                            .unwrap();
                        let mut guard = subs.lock().await;
                        for s in guard.iter_mut() {
                            s.recv().await.unwrap();
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

/// Single subscriber, raw wire round-trip.
fn bench_grpc_roundtrip(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (publisher, sub) = rt.block_on(async {
        let channel = start_broker().await;
        let publisher = Arc::new(Publisher::new(channel.clone()));
        let sub = Subscriber::new(channel.clone()).await.unwrap();
        sub.subscribe("bench.grpc.rt", None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        (publisher, Arc::new(Mutex::new(sub)))
    });

    c.bench_function("grpc_single_roundtrip", |b| {
        b.to_async(&rt).iter(|| {
            let publisher = Arc::clone(&publisher);
            let sub = Arc::clone(&sub);
            async move {
                publisher.publish("bench.grpc.rt", payload()).await.unwrap();
                sub.lock().await.recv().await.unwrap();
            }
        });
    });
}

/// Burst publish: N messages published back-to-back before draining.
/// Measures time per message to deliver a whole batch. Exercises the
/// batching forwarder — publisher outruns consumer, so the server's
/// `try_recv` drain collapses multiple deliveries into one frame.
fn bench_grpc_burst_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("grpc_burst_throughput");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    for &burst in &[10usize, 100, 1000] {
        group.throughput(criterion::Throughput::Elements(burst as u64));
        group.bench_with_input(
            BenchmarkId::new("msgs", burst),
            &burst,
            |b, &burst| {
                let (publisher, sub) = rt.block_on(async {
                    let channel = start_broker().await;
                    let publisher = Arc::new(Publisher::new(channel.clone()));
                    let sub = Subscriber::new(channel.clone()).await.unwrap();
                    sub.subscribe("bench.grpc.burst", None).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    (publisher, Arc::new(Mutex::new(sub)))
                });

                b.to_async(&rt).iter(|| {
                    let publisher = Arc::clone(&publisher);
                    let sub = Arc::clone(&sub);
                    async move {
                        for _ in 0..burst {
                            publisher.publish("bench.grpc.burst", payload()).await.unwrap();
                        }
                        let mut guard = sub.lock().await;
                        for _ in 0..burst {
                            guard.recv().await.unwrap();
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_grpc_fanout_e2e,
    bench_grpc_roundtrip,
    bench_grpc_burst_throughput
);
criterion_main!(benches);
