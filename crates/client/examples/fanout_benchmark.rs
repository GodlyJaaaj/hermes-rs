//! Fanout benchmark: send n messages to n fanout subscribers and measure scaling.
//!
//! By default this starts an embedded broker.
//! You can also target an external broker with `--uri http://127.0.0.1:4222`.
//!
//! Example:
//!   cargo run -p scylla-broker-client --example fanout_benchmark -- --n-list 1,2,4,8,16

use std::env;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::StreamExt;
use scylla_broker_client::ScyllaBrokerClient;
use scylla_broker_core::Subject;
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
    let mut timeout_secs = 20_u64;
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
                    timeout_secs = args[i + 1].parse::<u64>().unwrap_or(20);
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
        scylla_broker_server::run(listener).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    addr
}

async fn run_one(client: &ScyllaBrokerClient, n: usize, cfg: &Config) -> BenchRow {
    let messages = n;
    let clients = n;
    let expected_deliveries = messages * clients;

    let subject = format!("bench.fanout.{n}.{}", uuid::Uuid::now_v7());
    let subject_json = Subject::from(subject.as_str()).to_json();

    let mut handles = Vec::with_capacity(clients);

    for _ in 0..clients {
        let c = client.clone();
        let subject_json_cloned = subject_json.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = c
                .subscribe_raw(&subject_json_cloned, &[])
                .await
                .map_err(|e| format!("subscribe_raw error: {e}"))?;

            let mut count = 0_usize;
            while count < messages {
                match stream.next().await {
                    Some(Ok(_env)) => {
                        count += 1;
                    }
                    Some(Err(e)) => return Err(format!("stream recv error: {e}")),
                    None => break,
                }
            }
            Ok::<usize, String>(count)
        }));
    }

    tokio::time::sleep(Duration::from_millis(cfg.setup_delay_ms)).await;

    let publish_subject = Subject::from(subject.as_str());
    let start = Instant::now();

    for i in 0..messages {
        if let Err(e) = client.publish_raw(&publish_subject, b"x".to_vec()).await {
            eprintln!("publish error at message {i}: {e}");
            break;
        }
    }

    let timeout = Duration::from_secs(cfg.timeout_secs);
    let mut received_deliveries = 0_usize;

    for handle in handles {
        match tokio::time::timeout(timeout, handle).await {
            Ok(joined) => match joined {
                Ok(Ok(count)) => {
                    received_deliveries += count;
                }
                Ok(Err(err)) => {
                    eprintln!("subscriber task error: {err}");
                }
                Err(join_err) => {
                    eprintln!("subscriber join error: {join_err}");
                }
            },
            Err(_) => {
                eprintln!("subscriber timeout after {}s", cfg.timeout_secs);
            }
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
        "| n (messages=clients) | expected deliveries | received deliveries | completion | elapsed ms | deliveries/s | publish/s |"
    );
    println!(
        "|---:|---:|---:|---:|---:|---:|---:|"
    );

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

    let client = ScyllaBrokerClient::connect(uri).await?;

    let mut rows = Vec::with_capacity(cfg.n_list.len());
    for n in &cfg.n_list {
        println!("Running fanout row with n={n} ...");
        let row = run_one(&client, *n, &cfg).await;
        rows.push(row);
    }

    print_table(&rows);
    Ok(())
}

