//! Criterion benchmarks for the full gRPC stack (client ↔ embedded server).
//!
//! Run with: `cargo bench -p hermes-client`

use std::time::Duration;

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::Subject;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn start_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        hermes_server::run(listener).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    format!("http://{addr}")
}

// ---------------------------------------------------------------------------
// 1. Batch publish throughput via single gRPC stream
// ---------------------------------------------------------------------------

fn bench_batch_publish_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_batch_publish");
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    let rt = make_runtime();

    // Start server once for the group
    let uri = rt.block_on(start_server());
    let client = rt.block_on(HermesClient::connect(&uri)).unwrap();

    for n in [100_u64, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("messages", n), &n, |b, &n| {
            let subject = Subject::new().str("bench").str("batch");
            let payload = vec![0u8; 128];

            b.to_async(&rt).iter_custom(|iters| {
                let client = client.clone();
                let subject = subject.clone();
                let payload = payload.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let batch = client.batch_publisher();
                        let start = std::time::Instant::now();
                        for _ in 0..n {
                            batch.send_raw(&subject, payload.clone()).await.unwrap();
                        }
                        let _ = batch.flush().await;
                        total += start.elapsed();
                    }
                    total
                }
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 2. End-to-end single message latency
// ---------------------------------------------------------------------------

fn bench_e2e_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_e2e_latency");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(100);
    group.throughput(Throughput::Elements(1));

    let rt = make_runtime();
    let uri = rt.block_on(start_server());
    let client = rt.block_on(HermesClient::connect(&uri)).unwrap();

    let subject = Subject::new().str("bench").str("latency");
    let subject_json = subject.to_json();
    let payload = vec![0u8; 64];

    // Create a persistent subscriber stream
    let mut stream = rt
        .block_on(client.subscribe_raw(&subject_json, &[]))
        .unwrap();

    group.bench_function("publish_receive_one", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    client
                        .publish_raw(&subject, payload.clone())
                        .await
                        .unwrap();
                    let _ = stream.next().await.unwrap().unwrap();
                    total += start.elapsed();
                }
                total
            })
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. gRPC fanout: N subscribers receiving M messages
// ---------------------------------------------------------------------------

fn bench_grpc_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_fanout");
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20);

    let rt = make_runtime();
    let uri = rt.block_on(start_server());
    let client = rt.block_on(HermesClient::connect(&uri)).unwrap();

    let num_messages = 1000_u64;

    for num_subs in [1_u64, 5, 10, 50] {
        let expected = num_messages * num_subs;
        group.throughput(Throughput::Elements(expected));

        group.bench_with_input(
            BenchmarkId::new("subscribers", num_subs),
            &num_subs,
            |b, &num_subs| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = Duration::ZERO;

                        for iter_idx in 0..iters {
                            // Unique subject per iteration to avoid cross-iteration interference
                            let subject = Subject::new()
                                .str("bench")
                                .str("grpcfan")
                                .str(&format!("{num_subs}"))
                                .str(&format!("i{iter_idx}"));
                            let subject_json = subject.to_json();

                            // Subscribe N clients
                            let mut handles = Vec::with_capacity(num_subs as usize);
                            for _ in 0..num_subs {
                                let c = client.clone();
                                let sj = subject_json.clone();
                                handles.push(tokio::spawn(async move {
                                    let mut stream = c.subscribe_raw(&sj, &[]).await.unwrap();
                                    let mut count = 0_u64;
                                    while count < num_messages {
                                        if stream.next().await.is_some() {
                                            count += 1;
                                        } else {
                                            break;
                                        }
                                    }
                                    count
                                }));
                            }

                            // Let subscribers connect
                            tokio::time::sleep(Duration::from_millis(50)).await;

                            // Publish via batch
                            let start = std::time::Instant::now();

                            let batch = client.batch_publisher();
                            for _ in 0..num_messages {
                                batch.send_raw(&subject, vec![0u8; 64]).await.unwrap();
                            }
                            let _ = batch.flush().await;

                            // Wait for all subscribers
                            for handle in handles {
                                let _ = tokio::time::timeout(
                                    Duration::from_secs(10),
                                    handle,
                                )
                                .await;
                            }

                            total += start.elapsed();
                        }

                        total
                    })
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        bench_batch_publish_throughput,
        bench_e2e_latency,
        bench_grpc_fanout,
}
criterion_main!(benches);
