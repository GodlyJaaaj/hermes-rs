use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hermes_broker_core::router::{Router, RouterCmd, RouterConfig};
use hermes_broker_core::slot::SubHandle;
use tokio::runtime::Runtime;
use tokio::sync::{Notify, oneshot};

fn make_payload() -> Bytes {
    Bytes::from(vec![0xABu8; 128])
}

/// Tracks the highest sequence seen across all subscribers.
/// Once all N subscribers have seen the target sequence, notifies.
struct SeqTracker {
    /// Number of subscribers that have seen the target sequence.
    done_count: AtomicU64,
    n_subs: u64,
    notify: Notify,
}

impl SeqTracker {
    fn new(n_subs: u64) -> Arc<Self> {
        Arc::new(Self {
            done_count: AtomicU64::new(0),
            n_subs,
            notify: Notify::new(),
        })
    }

    /// Called by a subscriber when it sees the target sequence.
    fn mark_done(&self) {
        let prev = self.done_count.fetch_add(1, Ordering::Release);
        if prev + 1 >= self.n_subs {
            self.notify.notify_one();
        }
    }

    async fn wait_all_done(&self) {
        while self.done_count.load(Ordering::Acquire) < self.n_subs {
            self.notify.notified().await;
        }
    }
}

fn bench_fanout_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fanout_e2e");

    for n_subs in [1, 10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("subscribers", n_subs),
            &n_subs,
            |b, &n_subs| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let tracker = SeqTracker::new(n_subs as u64);
                    let target_seq = iters; // last published message's sequence

                    let (router, tx) = Router::new(RouterConfig::default(), 8192);
                    tokio::spawn(router.run());

                    // Subscribe N fanout subscribers.
                    for _ in 0..n_subs {
                        let (reply_tx, reply_rx) = oneshot::channel();
                        tx.send(RouterCmd::Subscribe {
                            subject: Box::from("bench.fanout.subject"),
                            queue_group: None,
                            reply: reply_tx,
                        })
                        .await
                        .unwrap();

                        let tracker = tracker.clone();
                        match reply_rx.await.unwrap() {
                            SubHandle::Fanout { mut rx, .. } => {
                                tokio::spawn(async move {
                                    loop {
                                        match rx.recv().await {
                                            Ok(d) => {
                                                if d.sequence >= target_seq {
                                                    tracker.mark_done();
                                                    return;
                                                }
                                            }
                                            Err(
                                                tokio::sync::broadcast::error::RecvError::Lagged(_),
                                            ) => continue,
                                            Err(
                                                tokio::sync::broadcast::error::RecvError::Closed,
                                            ) => return,
                                        }
                                    }
                                });
                            }
                            _ => unreachable!(),
                        }
                    }

                    let payload = make_payload();
                    let subject: Box<str> = Box::from("bench.fanout.subject");

                    let start = std::time::Instant::now();

                    let pub_handle = tokio::spawn(async move {
                        for _ in 0..iters {
                            tx.send(RouterCmd::Publish {
                                subject: subject.clone(),
                                payload: payload.clone(),
                                reply_to: None,
                            })
                            .await
                            .unwrap();
                        }
                    });

                    tracker.wait_all_done().await;
                    let elapsed = start.elapsed();
                    let _ = pub_handle.await;
                    elapsed
                });
            },
        );
    }

    group.finish();
}

/// Publisher-side throughput: how long does it take the router to *process*
/// `iters` publishes, independent of consumer wake-up cost?
///
/// Design: N background subscribers just drain their broadcast receivers as
/// fast as possible (we never wait for them). One dedicated sentinel
/// subscriber is measured until it sees the final sequence — its latency is
/// the signal, and since `tokio::sync::broadcast::Sender::send` is O(1) in N,
/// the curve should stay flat as N grows (unlike `bench_fanout_e2e`, which
/// waits for all N receivers and therefore pays N wake-ups per iteration).
fn bench_fanout_publish_only(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fanout_publish_only");

    for n_subs in [1, 10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("background_subs", n_subs),
            &n_subs,
            |b, &n_subs| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let (router, tx) = Router::new(RouterConfig::default(), 8192);
                    tokio::spawn(router.run());

                    // N background subscribers: drain and discard, never synchronized on.
                    for _ in 0..n_subs {
                        let (reply_tx, reply_rx) = oneshot::channel();
                        tx.send(RouterCmd::Subscribe {
                            subject: Box::from("bench.publish.subject"),
                            queue_group: None,
                            reply: reply_tx,
                        })
                        .await
                        .unwrap();

                        match reply_rx.await.unwrap() {
                            SubHandle::Fanout { mut rx, .. } => {
                                tokio::spawn(async move {
                                    loop {
                                        match rx.recv().await {
                                            Ok(_) => {}
                                            Err(
                                                tokio::sync::broadcast::error::RecvError::Lagged(_),
                                            ) => continue,
                                            Err(
                                                tokio::sync::broadcast::error::RecvError::Closed,
                                            ) => return,
                                        }
                                    }
                                });
                            }
                            _ => unreachable!(),
                        }
                    }

                    // One sentinel subscriber — its "last seen" signal tells us
                    // the router has finished processing all publishes.
                    let tracker = SeqTracker::new(1);
                    let target_seq = iters;
                    let (reply_tx, reply_rx) = oneshot::channel();
                    tx.send(RouterCmd::Subscribe {
                        subject: Box::from("bench.publish.subject"),
                        queue_group: None,
                        reply: reply_tx,
                    })
                    .await
                    .unwrap();

                    let t = tracker.clone();
                    match reply_rx.await.unwrap() {
                        SubHandle::Fanout { mut rx, .. } => {
                            tokio::spawn(async move {
                                loop {
                                    match rx.recv().await {
                                        Ok(d) => {
                                            if d.sequence >= target_seq {
                                                t.mark_done();
                                                return;
                                            }
                                        }
                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(
                                            _,
                                        )) => continue,
                                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                            return;
                                        }
                                    }
                                }
                            });
                        }
                        _ => unreachable!(),
                    }

                    let payload = make_payload();
                    let subject: Box<str> = Box::from("bench.publish.subject");

                    let start = std::time::Instant::now();

                    let pub_handle = tokio::spawn(async move {
                        for _ in 0..iters {
                            tx.send(RouterCmd::Publish {
                                subject: subject.clone(),
                                payload: payload.clone(),
                                reply_to: None,
                            })
                            .await
                            .unwrap();
                        }
                    });

                    tracker.wait_all_done().await;
                    let elapsed = start.elapsed();
                    let _ = pub_handle.await;
                    elapsed
                });
            },
        );
    }

    group.finish();
}

fn bench_queue_group_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("queue_group_4_members_e2e", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let tracker = SeqTracker::new(1); // only 1 member receives each msg
            let target_seq = iters;

            let (router, tx) = Router::new(RouterConfig::default(), 8192);
            tokio::spawn(router.run());

            // We track the last sequence seen across ALL members.
            // Since round-robin, the last message goes to one member.
            let max_seq = Arc::new(AtomicU64::new(0));

            for _ in 0..4 {
                let (reply_tx, reply_rx) = oneshot::channel();
                tx.send(RouterCmd::Subscribe {
                    subject: Box::from("bench.queue"),
                    queue_group: Some(Box::from("workers")),
                    reply: reply_tx,
                })
                .await
                .unwrap();

                let tracker = tracker.clone();
                let max_seq = max_seq.clone();
                match reply_rx.await.unwrap() {
                    SubHandle::QueueMember { rx, .. } => {
                        tokio::spawn(async move {
                            while let Ok(d) = rx.recv().await {
                                let prev = max_seq.fetch_max(d.sequence, Ordering::Release);
                                if d.sequence >= target_seq || prev >= target_seq {
                                    tracker.mark_done();
                                    return;
                                }
                            }
                        });
                    }
                    _ => unreachable!(),
                }
            }

            let payload = make_payload();
            let start = std::time::Instant::now();

            let pub_handle = tokio::spawn(async move {
                for _ in 0..iters {
                    tx.send(RouterCmd::Publish {
                        subject: Box::from("bench.queue"),
                        payload: payload.clone(),
                        reply_to: None,
                    })
                    .await
                    .unwrap();
                }
            });

            tracker.wait_all_done().await;
            let elapsed = start.elapsed();
            let _ = pub_handle.await;
            elapsed
        });
    });
}

fn bench_wildcard_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("wildcard_routing_e2e", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let tracker = SeqTracker::new(1);
            let target_seq = iters;

            let (router, tx) = Router::new(RouterConfig::default(), 8192);
            tokio::spawn(router.run());

            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(RouterCmd::Subscribe {
                subject: Box::from("bench.*.data"),
                queue_group: None,
                reply: reply_tx,
            })
            .await
            .unwrap();

            let t = tracker.clone();
            match reply_rx.await.unwrap() {
                SubHandle::Fanout { mut rx, .. } => {
                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Ok(d) => {
                                    if d.sequence >= target_seq {
                                        t.mark_done();
                                        return;
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                    continue;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                            }
                        }
                    });
                }
                _ => unreachable!(),
            }

            let payload = make_payload();
            let start = std::time::Instant::now();

            let pub_handle = tokio::spawn(async move {
                for _ in 0..iters {
                    tx.send(RouterCmd::Publish {
                        subject: Box::from("bench.region1.data"),
                        payload: payload.clone(),
                        reply_to: None,
                    })
                    .await
                    .unwrap();
                }
            });

            tracker.wait_all_done().await;
            let elapsed = start.elapsed();
            let _ = pub_handle.await;
            elapsed
        });
    });
}

criterion_group!(
    benches,
    bench_fanout_e2e,
    bench_fanout_publish_only,
    bench_queue_group_e2e,
    bench_wildcard_e2e,
);
criterion_main!(benches);
