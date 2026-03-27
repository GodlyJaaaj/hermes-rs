//! Queue group example — load-balanced consumption.
//!
//! 1. Start the broker: `cargo run -p scylla-broker-server`
//! 2. In another terminal: `cargo run -p scylla-broker-client --example queue_group`

use futures::StreamExt;
use scylla_broker_client::ScyllaBrokerClient;
use scylla_broker_core::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Event)]
struct Task {
    id: u32,
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ScyllaBrokerClient::connect("http://127.0.0.1:4222").await?;

    // Spawn 3 workers in the same queue group
    for worker_id in 0..3 {
        let worker_client = client.clone();
        tokio::spawn(async move {
            let mut stream = worker_client
                .subscribe::<Task>(&["task-workers"])
                .await
                .unwrap();
            while let Some(result) = stream.next().await {
                match result {
                    Ok(task) => {
                        println!(
                            "Worker {worker_id} processing task #{}: {}",
                            task.id, task.payload
                        );
                    }
                    Err(e) => eprintln!("Worker {worker_id} error: {e}"),
                }
            }
        });
    }

    // Give workers time to subscribe
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publish 10 tasks — each will be received by exactly one worker
    for i in 0..10 {
        client
            .publish(&Task {
                id: i,
                payload: format!("do-something-{i}"),
            })
            .await?;
    }

    println!("Published 10 tasks to queue group 'task-workers'");

    // Wait for processing
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}
