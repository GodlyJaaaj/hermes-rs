//! Criterion benchmarks for the BrokerEngine (in-process, no gRPC overhead).
//!
//! Run with: `cargo bench -p hermes-server`

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hermes_core::Subject;
use hermes_proto::EventEnvelope;
use hermes_server::broker::BrokerEngine;
use hermes_server::subscription::SubscriptionReceiver;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_envelope(subject: &Subject, payload_size: usize) -> EventEnvelope {
    EventEnvelope {
        id: "bench-msg-0".into(),
        subject: subject.to_bytes(),
        payload: vec![0u8; payload_size],
        headers: HashMap::new(),
        timestamp_nanos: 0,
    }
}

fn drain_receiver(rx: &mut SubscriptionReceiver) {
    match rx {
        SubscriptionReceiver::Fanout(r) => while r.try_recv().is_ok() {},
        SubscriptionReceiver::QueueGroup(r) => while r.try_recv().is_ok() {},
    }
}

// ---------------------------------------------------------------------------
// 1. Fanout scaling: how publish() scales with subscriber count
// ---------------------------------------------------------------------------

fn bench_publish_fanout_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_fanout_scaling");
    let num_messages: u64 = 1000;

    for k in [0, 1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(num_messages));
        group.bench_with_input(BenchmarkId::new("subscribers", k), &k, |b, &k| {
            let engine = BrokerEngine::new(16384, None);
            let subject = Subject::new().str("bench").str("fanout");
            let envelope = make_envelope(&subject, 128);

            let mut receivers: Vec<SubscriptionReceiver> = (0..k)
                .map(|_| engine.subscribe(subject.clone(), vec![]).1)
                .collect();

            b.iter_custom(|iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    for _ in 0..num_messages {
                        black_box(engine.publish(&envelope));
                    }
                }
                let elapsed = start.elapsed();
                // Drain outside of timing
                for rx in &mut receivers {
                    drain_receiver(rx);
                }
                elapsed
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Message sizes: throughput with varying payloads
// ---------------------------------------------------------------------------

fn bench_publish_message_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_message_sizes");
    let num_messages: u64 = 1000;

    for payload_size in [64_usize, 256, 1024, 4096, 16384, 65536] {
        group.throughput(Throughput::Bytes(payload_size as u64 * num_messages));
        group.bench_with_input(
            BenchmarkId::new("bytes", payload_size),
            &payload_size,
            |b, &sz| {
                let engine = BrokerEngine::new(16384, None);
                let subject = Subject::new().str("bench").str("sizes");
                let envelope = make_envelope(&subject, sz);

                // One subscriber to exercise the broadcast path
                let mut rx = engine.subscribe(subject.clone(), vec![]).1;

                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        for _ in 0..num_messages {
                            black_box(engine.publish(&envelope));
                        }
                    }
                    let elapsed = start.elapsed();
                    drain_receiver(&mut rx);
                    elapsed
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Queue group vs fanout
// ---------------------------------------------------------------------------

fn bench_queue_group_vs_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_group_vs_fanout");
    let num_messages: u64 = 1000;
    let members = 10;

    group.throughput(Throughput::Elements(num_messages));

    // Fanout: 10 subscribers, each gets every message
    group.bench_function("fanout_10", |b| {
        let engine = BrokerEngine::new(16384, None);
        let subject = Subject::new().str("bench").str("fanout10");
        let envelope = make_envelope(&subject, 128);

        let mut receivers: Vec<SubscriptionReceiver> = (0..members)
            .map(|_| engine.subscribe(subject.clone(), vec![]).1)
            .collect();

        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for _ in 0..num_messages {
                    black_box(engine.publish(&envelope));
                }
            }
            let elapsed = start.elapsed();
            for rx in &mut receivers {
                drain_receiver(rx);
            }
            elapsed
        });
    });

    // Queue group: 10 members, round-robin (one gets each message)
    group.bench_function("queue_group_10", |b| {
        let engine = BrokerEngine::new(16384, None);
        let subject = Subject::new().str("bench").str("qg10");
        let envelope = make_envelope(&subject, 128);

        let mut receivers: Vec<SubscriptionReceiver> = (0..members)
            .map(|_| engine.subscribe(subject.clone(), vec!["workers".into()]).1)
            .collect();

        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for _ in 0..num_messages {
                    black_box(engine.publish(&envelope));
                }
            }
            let elapsed = start.elapsed();
            for rx in &mut receivers {
                drain_receiver(rx);
            }
            elapsed
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Wildcard matching overhead
// ---------------------------------------------------------------------------

fn bench_wildcard_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("wildcard_matching");
    let num_messages: u64 = 1000;
    group.throughput(Throughput::Elements(num_messages));

    // Exact match: subscriber on ["bench","wc","test"], publish same
    group.bench_function("exact_match", |b| {
        let engine = BrokerEngine::new(16384, None);
        let subject = Subject::new().str("bench").str("wc").str("test");
        let envelope = make_envelope(&subject, 64);

        let mut rx = engine.subscribe(subject.clone(), vec![]).1;

        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for _ in 0..num_messages {
                    black_box(engine.publish(&envelope));
                }
            }
            let elapsed = start.elapsed();
            drain_receiver(&mut rx);
            elapsed
        });
    });

    // Single wildcard: subscriber on ["bench","*","test"]
    group.bench_function("single_wildcard", |b| {
        let engine = BrokerEngine::new(16384, None);
        let sub_pattern = Subject::new().str("bench").any().str("test");
        let _rx = engine.subscribe(sub_pattern.clone(), vec![]).1;

        let pub_subject = Subject::new().str("bench").str("wc").str("test");
        let envelope = make_envelope(&pub_subject, 64);

        // Need to also drain the subscriber (it's a wildcard receiver)
        // Actually we need to keep the receiver to prevent it from being dropped
        // Let's re-do this properly
        drop(_rx);

        let mut rx = engine
            .subscribe(Subject::new().str("bench").any().str("test"), vec![])
            .1;

        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for _ in 0..num_messages {
                    black_box(engine.publish(&envelope));
                }
            }
            let elapsed = start.elapsed();
            drain_receiver(&mut rx);
            elapsed
        });
    });

    // Multi-wildcard: subscriber on ["bench",">"]
    group.bench_function("multi_wildcard", |b| {
        let engine = BrokerEngine::new(16384, None);
        let sub_subject = Subject::new().str("bench").rest();
        let mut rx = engine.subscribe(sub_subject, vec![]).1;

        let pub_subject = Subject::new().str("bench").str("wc").str("test");
        let envelope = make_envelope(&pub_subject, 64);

        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for _ in 0..num_messages {
                    black_box(engine.publish(&envelope));
                }
            }
            let elapsed = start.elapsed();
            drain_receiver(&mut rx);
            elapsed
        });
    });

    // No-match wildcard: wildcards exist but don't match the published subject
    group.bench_function("no_match_wildcard", |b| {
        let engine = BrokerEngine::new(16384, None);
        // Add 100 wildcard subscriptions on OTHER subjects
        let _others: Vec<SubscriptionReceiver> = (0..100)
            .map(|i| {
                let sub = Subject::new().str("other").str(i.to_string()).rest();
                engine.subscribe(sub, vec![]).1
            })
            .collect();

        let pub_subject = Subject::new().str("bench").str("wc").str("test");
        let envelope = make_envelope(&pub_subject, 64);

        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for _ in 0..num_messages {
                    black_box(engine.publish(&envelope));
                }
            }

            start.elapsed()
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Subscribe/unsubscribe churn
// ---------------------------------------------------------------------------

fn bench_subscribe_unsubscribe_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscribe_unsubscribe");
    let num_ops: u64 = 500;

    group.throughput(Throughput::Elements(num_ops));

    group.bench_function("subscribe_only", |b| {
        b.iter_custom(|iters| {
            let engine = BrokerEngine::new(8192, None);
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for i in 0..num_ops {
                    let sub = Subject::new().str("churn").str(i.to_string());
                    let _ = black_box(engine.subscribe(sub, vec![]));
                }
            }
            start.elapsed()
        });
    });

    group.bench_function("subscribe_unsubscribe_cycle", |b| {
        b.iter_custom(|iters| {
            let engine = BrokerEngine::new(8192, None);
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for i in 0..num_ops {
                    let sub = Subject::new().str("churn").str(i.to_string());
                    let (id, _rx) = engine.subscribe(sub.clone(), vec![]);
                    engine.unsubscribe(&sub, id);
                }
            }
            start.elapsed()
        });
    });

    group.bench_function("subscribe_same_subject_qg", |b| {
        b.iter_custom(|iters| {
            let engine = BrokerEngine::new(8192, None);
            let sub = Subject::new().str("churn").str("same");
            let start = std::time::Instant::now();
            for _ in 0..iters {
                for _ in 0..num_ops {
                    let _ = black_box(engine.subscribe(sub.clone(), vec!["workers".into()]));
                }
            }
            start.elapsed()
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Publish baseline (no subscribers)
// ---------------------------------------------------------------------------

fn bench_publish_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_baseline");
    let num_messages: u64 = 10_000;
    group.throughput(Throughput::Elements(num_messages));

    group.bench_function("no_subscribers", |b| {
        let engine = BrokerEngine::new(8192, None);
        let subject = Subject::new().str("bench").str("baseline");
        let envelope = make_envelope(&subject, 64);

        b.iter(|| {
            for _ in 0..num_messages {
                black_box(engine.publish(&envelope));
            }
        });
    });

    group.bench_function("no_match_100_wildcards", |b| {
        let engine = BrokerEngine::new(8192, None);

        // 100 wildcard subscriptions on other subjects
        let _others: Vec<SubscriptionReceiver> = (0..100)
            .map(|i| {
                let sub = Subject::new().str("noise").str(i.to_string()).rest();
                engine.subscribe(sub, vec![]).1
            })
            .collect();

        let subject = Subject::new().str("bench").str("baseline");
        let envelope = make_envelope(&subject, 64);

        b.iter(|| {
            for _ in 0..num_messages {
                black_box(engine.publish(&envelope));
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. Large-scale fanout (10K+ subscribers)
// ---------------------------------------------------------------------------

fn bench_large_scale_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_fanout");
    group.measurement_time(Duration::from_secs(15));
    let num_messages: u64 = 500;

    for k in [10_000_u64, 100_000] {
        group.throughput(Throughput::Elements(num_messages));
        group.bench_with_input(BenchmarkId::new("subscribers", k), &k, |b, &k| {
            let engine = BrokerEngine::new(k as usize + 1024, None);
            let subject = Subject::new().str("bench").str("largefanout");
            let envelope = make_envelope(&subject, 128);

            let mut receivers: Vec<SubscriptionReceiver> = (0..k)
                .map(|_| engine.subscribe(subject.clone(), vec![]).1)
                .collect();

            b.iter_custom(|iters| {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    for _ in 0..num_messages {
                        black_box(engine.publish(&envelope));
                    }
                }
                let elapsed = start.elapsed();
                for rx in &mut receivers {
                    drain_receiver(rx);
                }
                elapsed
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 8. Lookup throughput (batches of 10K publishes, varying subscriber counts)
// ---------------------------------------------------------------------------

fn bench_lookup_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_throughput");
    group.measurement_time(Duration::from_secs(15));
    let batch: u64 = 10_000;

    for num_subs in [100_u64, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(batch));
        group.bench_with_input(
            BenchmarkId::new("subscriptions", num_subs),
            &num_subs,
            |b, &n| {
                let engine = BrokerEngine::new(n as usize + 1024, None);

                // Create N subscriptions on different exact subjects
                let _receivers: Vec<SubscriptionReceiver> = (0..n)
                    .map(|i| {
                        let sub = Subject::new().str("topic").str(i.to_string());
                        engine.subscribe(sub, vec![]).1
                    })
                    .collect();

                // Publish to a subject that matches the last subscription
                let target = Subject::new().str("topic").str((n - 1).to_string());
                let envelope = make_envelope(&target, 64);

                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        for _ in 0..batch {
                            black_box(engine.publish(&envelope));
                        }
                    }
                    start.elapsed()
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 9. Large-scale wildcard matching (many wildcard patterns)
// ---------------------------------------------------------------------------

fn bench_large_scale_wildcards(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_wildcards");
    group.measurement_time(Duration::from_secs(15));
    let num_messages: u64 = 1000;

    for num_patterns in [10_u64, 100, 1000] {
        group.throughput(Throughput::Elements(num_messages));
        group.bench_with_input(
            BenchmarkId::new("wildcard_patterns", num_patterns),
            &num_patterns,
            |b, &n| {
                let engine = BrokerEngine::new(16384, None);

                // Create N wildcard subscriptions: "sensor.{i}.*"
                let _receivers: Vec<SubscriptionReceiver> = (0..n)
                    .map(|i| {
                        let sub = Subject::new().str("sensor").str(i.to_string()).any();
                        engine.subscribe(sub, vec![]).1
                    })
                    .collect();

                // Publish to a matching subject: "sensor.0.temperature"
                let pub_subject = Subject::new().str("sensor").str("0").str("temperature");
                let envelope = make_envelope(&pub_subject, 64);

                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        for _ in 0..num_messages {
                            black_box(engine.publish(&envelope));
                        }
                    }
                    start.elapsed()
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
    config = Criterion::default().measurement_time(Duration::from_secs(10));
    targets =
        bench_publish_fanout_scaling,
        bench_publish_message_sizes,
        bench_queue_group_vs_fanout,
        bench_wildcard_matching,
        bench_subscribe_unsubscribe_churn,
        bench_publish_baseline,
}

criterion_group! {
    name = large_scale;
    config = Criterion::default().measurement_time(Duration::from_secs(15));
    targets =
        bench_large_scale_fanout,
        bench_lookup_throughput,
        bench_large_scale_wildcards,
}

criterion_main!(benches, large_scale);
