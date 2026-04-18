use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use bytes::Bytes;
use hermes_broker_core::router::{Router, RouterCmd, RouterConfig};
use hermes_broker_core::slot::{SessionId, SubHandle};
use tokio::sync::{Notify, oneshot};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    for &n_subs in &[1usize, 10, 100, 1000] {
        let (router, tx) = Router::new(RouterConfig::default(), 8192);
        tokio::spawn(router.run());

        let iters: u64 = 10_000;
        let done = Arc::new(AtomicU64::new(0));
        let notify = Arc::new(Notify::new());

        for _ in 0..n_subs {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(RouterCmd::Subscribe {
                subject: Box::from("bench.subj"),
                queue_group: None,
                session_id: SessionId(0),
                reply: reply_tx,
            }).await.unwrap();
            let d = done.clone();
            let n = notify.clone();
            match reply_rx.await.unwrap() {
                SubHandle::Fanout { mut rx, .. } => {
                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Ok(m) => {
                                    if m.sequence >= iters {
                                        if d.fetch_add(1, Ordering::Release) + 1 >= n_subs as u64 {
                                            n.notify_one();
                                        }
                                        return;
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(_) => return,
                            }
                        }
                    });
                }
                _ => unreachable!(),
            }
        }

        let payload = Bytes::from(vec![0xABu8; 128]);
        let start = Instant::now();
        for _ in 0..iters {
            tx.send(RouterCmd::Publish {
                subject: Box::from("bench.subj"),
                payload: payload.clone(),
                reply_to: None,
            }).await.unwrap();
        }
        let publish_done = start.elapsed();

        // wait for all subs to see target
        while done.load(Ordering::Acquire) < n_subs as u64 {
            tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified()).await.unwrap();
        }
        let e2e_done = start.elapsed();

        let pub_ns_per = publish_done.as_nanos() as f64 / iters as f64;
        let e2e_ns_per = e2e_done.as_nanos() as f64 / iters as f64;
        println!("n_subs={n_subs:5}  publish={pub_ns_per:8.0} ns/msg  e2e={e2e_ns_per:8.0} ns/msg");
    }
}
