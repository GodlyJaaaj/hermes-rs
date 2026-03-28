//! Fanout benchmark: send n messages to n fanout subscribers and measure scaling.
//!
//! Uses `batch_publisher` (single gRPC stream) for high-throughput publishing.
//! Build with `--release` for realistic numbers:
//!
//!   cargo run --release -p hermes-integration-tests --example fanout_benchmark -- --n-list 10,100,1000
//!
//! Use `--uri` for an external broker:
//!   cargo run --release -p hermes-integration-tests --example fanout_benchmark -- --uri http://127.0.0.1:4222 --n-list 1000

use std::env;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::Subject;
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
struct Config {
    uri: Option<String>,
    n_list: Vec<usize>,
    timeout_secs: u64,
    setup_delay_ms: u64,
}

#[derive(Debug, Clone)]
struct BenchRow {
    n: usize,
    expected_deliveries: usize,
    received_deliveries: usize,
    elapsed: Duration,
}

impl BenchRow {
    fn delivery_ratio(&self) -> f64 {
        if self.expected_deliveries == 0 {
            return 0.0;
        }
        (self.received_deliveries as f64) / (self.expected_deliveries as f64)
    }

    fn deliveries_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs == 0.0 {
            return 0.0;
        }
        (self.received_deliveries as f64) / secs
    }

    fn publish_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs == 0.0 {
            return 0.0;
        }
        (self.n as f64) / secs
    }
}

fn parse_config() -> Config {
    let mut uri = None;
    let mut n_list = vec![1, 2, 4, 8, 16, 32];
    let mut timeout_secs = 30_u64;
    let mut setup_delay_ms = 120_u64;

    let args: Vec<String> = env::args().collect();
    let mut i = 1_usize;

    while i < args.len() {
        match args[i].as_str() {
            "--uri" => {
                if i + 1 < args.len() {
                    uri = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Missing value for --uri");
                    std::process::exit(2);
                }
            }
            "--n-list" => {
                if i + 1 < args.len() {
                    let parsed = parse_n_list(&args[i + 1]);
                    if parsed.is_empty() {
                        eprintln!("--n-list must contain at least one positive integer");
                        std::process::exit(2);
                    }
                    n_list = parsed;
                    i += 2;
                } else {
                    eprintln!("Missing value for --n-list");
                    std::process::exit(2);
                }
            }
            "--timeout-secs" => {
                if i + 1 < args.len() {
                    timeout_secs = args[i + 1].parse::<u64>().unwrap_or(30);
                    i += 2;
                } else {
                    eprintln!("Missing value for --timeout-secs");
                    std::process::exit(2);
                }
            }
            "--setup-delay-ms" => {
                if i + 1 < args.len() {
                    setup_delay_ms = args[i + 1].parse::<u64>().unwrap_or(120);
                    i += 2;
                } else {
                    eprintln!("Missing value for --setup-delay-ms");
                    std::process::exit(2);
                }
            }
            "--help" | "-h" => {
                print_help_and_exit(0);
            }
            other => {
                eprintln!("Unknown argument: {other}");
                print_help_and_exit(2);
            }
        }
    }

    Config {
        uri,
        n_list,
        timeout_secs,
        setup_delay_ms,
    }
}

fn parse_n_list(value: &str) -> Vec<usize> {
    let mut out: Vec<usize> = value
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn print_help_and_exit(code: i32) -> ! {
    println!("fanout_benchmark options:");
    println!("  --uri <http://host:port>     Use external broker URI");
    println!("  --n-list <csv>               n values (clients=n, messages=n)");
    println!("  --timeout-secs <u64>         Max wait per benchmark row");
    println!("  --setup-delay-ms <u64>       Delay to let subscribers attach");
    println!("  -h, --help                   Show this help");
    std::process::exit(code);
}

async fn start_embedded_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        hermes_server::run(listener).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    addr
}

async fn run_one(client: &HermesClient, n: usize, cfg: &Config) -> BenchRow {
    let messages = n;
    let clients = n;
    let expected_deliveries = messages * clients;

    let subject_str = format!("bench.fanout.{n}.{}", uuid::Uuid::now_v7());
    let subject = Subject::from(subject_str.as_str());

    // --- Set up subscribers ---
    let mut handles = Vec::with_capacity(clients);

    for _ in 0..clients {
        let c = client.clone();
        let s = subject.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = c
                .subscribe_raw(&s, &[])
                .await
                .map_err(|e| format!("subscribe_raw error: {e}"))?;

            let mut count = 0_usize;
            while count < messages {
                match stream.next().await {
                    Some(Ok(_)) => count += 1,
                    Some(Err(e)) => return Err(format!("stream recv error: {e}")),
                    None => break,
                }
            }
            Ok::<usize, String>(count)
        }));
    }

    tokio::time::sleep(Duration::from_millis(cfg.setup_delay_ms)).await;

    // --- Publish via batch (single gRPC stream) ---
    let start = Instant::now();

    let batch = client.batch_publisher();
    for _ in 0..messages {
        if let Err(e) = batch.send_raw(&subject, b"x".to_vec()).await {
            eprintln!("batch send error: {e}");
            break;
        }
    }
    // Flush closes the stream and waits for the server ack.
    match batch.flush().await {
        Ok(ack) => {
            if ack.accepted != messages as u64 {
                eprintln!(
                    "warning: server accepted {} of {} messages",
                    ack.accepted, messages
                );
            }
        }
        Err(e) => eprintln!("batch flush error: {e}"),
    }

    // --- Wait for all subscribers ---
    let timeout = Duration::from_secs(cfg.timeout_secs);
    let mut received_deliveries = 0_usize;

    for handle in handles {
        match tokio::time::timeout(timeout, handle).await {
            Ok(Ok(Ok(count))) => received_deliveries += count,
            Ok(Ok(Err(err))) => eprintln!("subscriber task error: {err}"),
            Ok(Err(join_err)) => eprintln!("subscriber join error: {join_err}"),
            Err(_) => eprintln!("subscriber timeout after {}s", cfg.timeout_secs),
        }
    }

    BenchRow {
        n,
        expected_deliveries,
        received_deliveries,
        elapsed: start.elapsed(),
    }
}

fn print_table(rows: &[BenchRow]) {
    println!();
    println!(
        "| n (msgs=subs) | expected | received | completion | elapsed ms | deliveries/s | publish/s |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|");

    for row in rows {
        println!(
            "| {} | {} | {} | {:.2}% | {:.2} | {:.2} | {:.2} |",
            row.n,
            row.expected_deliveries,
            row.received_deliveries,
            row.delivery_ratio() * 100.0,
            row.elapsed.as_secs_f64() * 1000.0,
            row.deliveries_per_sec(),
            row.publish_per_sec()
        );
    }
    println!();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = parse_config();

    let uri = if let Some(uri) = cfg.uri.clone() {
        println!("Using external broker: {uri}");
        uri
    } else {
        let addr = start_embedded_broker().await;
        let uri = format!("http://{addr}");
        println!("Started embedded broker on {uri}");
        uri
    };

    let client = HermesClient::connect(uri).await?;

    let mut rows = Vec::with_capacity(cfg.n_list.len());
    for n in &cfg.n_list {
        println!("Running fanout row with n={n} ...");
        let row = run_one(&client, *n, &cfg).await;
        rows.push(row);
    }

    print_table(&rows);
    Ok(())
}
