//! Interactive TUI for the hermes broker.
//!
//! Starts an embedded broker (fire-and-forget + durable) and gives you a
//! full-screen terminal UI to publish, subscribe, ack, nack, etc.
//!
//! Run with embedded broker:
//!   cargo run -p hermes-integration-tests --example repl
//!
//! Connect to an external broker:
//!   cargo run -p hermes-integration-tests --example repl -- --connect http://127.0.0.1:4222
//!
//! Embedded broker without durable mode:
//!   cargo run -p hermes-integration-tests --example repl -- --no-store
//!
//! Press F1 for help inside the TUI.

use std::collections::HashMap;
use std::io::stdout;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{ExecutableCommand, execute};
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::{Event as BrokerEvent, Subject, event_group};
use ratatui::Terminal;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "hermes-repl", about = "Interactive TUI for the hermes broker")]
struct Cli {
    /// Connect to an external broker instead of starting an embedded one
    #[arg(short, long, value_name = "URL")]
    connect: Option<String>,

    /// Path to the redb store file for the embedded broker (default: temp file)
    #[arg(long, value_name = "PATH")]
    store_path: Option<String>,

    /// Disable durable mode for the embedded broker (fire-and-forget only)
    #[arg(long)]
    no_store: bool,
}

// ---------------------------------------------------------------------------
// Event types (for typed shortcuts)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BrokerEvent)]
struct ChatMessage {
    user: String,
    text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BrokerEvent)]
struct OrderPlaced {
    order_id: String,
    total: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BrokerEvent)]
struct OrderShipped {
    order_id: String,
    carrier: String,
}

event_group!(OrderEvents = [OrderPlaced, OrderShipped]);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BrokerEvent)]
struct Task {
    id: u32,
    payload: String,
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LogEntry {
    kind: LogKind,
    text: String,
}

#[derive(Clone, Copy)]
enum LogKind {
    System,
    Published,
    Received,
    Error,
}

struct App {
    input: String,
    cursor_pos: usize,
    messages: Vec<LogEntry>,
    subscriptions: HashMap<usize, String>,
    sub_handles: HashMap<usize, tokio::task::JoinHandle<()>>,
    scroll_offset: usize,
    should_quit: bool,
    sub_counter: Arc<AtomicUsize>,
    broker_uri: String,
    embedded: bool,
    durable_enabled: bool,
    pending_acks: HashMap<String, mpsc::Sender<hermes_proto::DurableClientMessage>>,
    history: Vec<String>,
    history_index: Option<usize>,
    saved_input: String,
}

impl App {
    fn new(broker_uri: String, embedded: bool, durable_enabled: bool) -> Self {
        Self {
            input: String::new(),
            cursor_pos: 0,
            messages: Vec::new(),
            subscriptions: HashMap::new(),
            sub_handles: HashMap::new(),
            scroll_offset: 0,
            should_quit: false,
            sub_counter: Arc::new(AtomicUsize::new(0)),
            broker_uri,
            embedded,
            durable_enabled,
            pending_acks: HashMap::new(),
            history: Vec::new(),
            history_index: None,
            saved_input: String::new(),
        }
    }

    fn log(&mut self, kind: LogKind, text: String) {
        self.messages.push(LogEntry { kind, text });
        self.scroll_to_bottom();
    }

    fn scroll_to_bottom(&mut self) {
        let len = self.messages.len();
        if len > 0 {
            self.scroll_offset = len.saturating_sub(1);
        }
    }

    fn next_sub_id(&self) -> usize {
        self.sub_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Convert char-based cursor_pos to byte offset in input string.
    fn cursor_byte_pos(&self) -> usize {
        self.input
            .char_indices()
            .nth(self.cursor_pos)
            .map(|(i, _)| i)
            .unwrap_or(self.input.len())
    }

    /// Number of chars in input.
    fn input_char_count(&self) -> usize {
        self.input.chars().count()
    }
}

// ---------------------------------------------------------------------------
// Events from async tasks -> UI
// ---------------------------------------------------------------------------

enum AppEvent {
    MessageReceived {
        sub_id: usize,
        subject: String,
        payload: String,
    },
    DurableReceived {
        sub_id: usize,
        subject: String,
        payload: String,
        msg_id: String,
        attempt: u32,
        auto_ack: bool,
        /// For manual mode: sender to ack/nack this message's stream
        ack_sender: Option<mpsc::Sender<hermes_proto::DurableClientMessage>>,
    },
    SubscriptionEnded(usize),
    Error {
        sub_id: usize,
        error: String,
    },
}

// ---------------------------------------------------------------------------
// Broker setup
// ---------------------------------------------------------------------------

async fn start_broker(cli: &Cli) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = hermes_server::config::ServerConfig::default();

    if cli.no_store {
        config.store_path = None;
    } else if let Some(ref path) = cli.store_path {
        config.store_path = Some(std::path::PathBuf::from(path));
    } else {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        config.store_path = Some(tmp.path().to_path_buf());
        // Keep the temp file alive by leaking it — it's cleaned up on process exit
        std::mem::forget(tmp);
    }

    config.redelivery_interval_secs = 2;
    config.default_ack_timeout_secs = 10;

    tokio::spawn(async move {
        hermes_server::run_with_config(listener, config)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn parse_subject(dot_subject: &str) -> Subject {
    Subject::from(dot_subject)
}

fn parse_groups(arg: Option<&str>) -> Vec<&str> {
    match arg {
        Some(s) if !s.is_empty() => s.split(',').collect(),
        _ => vec![],
    }
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

async fn execute_command(
    input: &str,
    client: &HermesClient,
    app: &mut App,
    event_tx: &mpsc::Sender<AppEvent>,
) {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return;
    }

    let cmd = parts[0];
    let args = &parts[1..];

    match cmd {
        "help" | "?" => cmd_help(app),
        "sub" => cmd_sub(client, app, args, event_tx).await,
        "pub" => cmd_pub(client, app, args).await,
        "chat" => cmd_chat(client, app, args).await,
        "order" => cmd_order(client, app, args).await,
        "ship" => cmd_ship(client, app, args).await,
        "task" => cmd_task(client, app, args).await,
        "batch" => cmd_batch(client, app, args).await,
        "dpub" => cmd_dpub(client, app, args).await,
        "dsub" => cmd_dsub(client, app, args, event_tx, true).await,
        "dsub-manual" => cmd_dsub(client, app, args, event_tx, false).await,
        "ack" => cmd_ack(app, args).await,
        "nack" => cmd_nack(app, args).await,
        "unsub" => cmd_unsub(app, args),
        "subs" => cmd_subs(app),
        "flood" => cmd_flood(client, app, args).await,
        "info" => cmd_info(app),
        "clear" => {
            app.messages.clear();
            app.scroll_offset = 0;
        }
        "quit" | "exit" | "q" => app.should_quit = true,
        _ => app.log(
            LogKind::Error,
            format!("Unknown command: {cmd}. Press F1 for help."),
        ),
    }
}

async fn cmd_sub(
    client: &HermesClient,
    app: &mut App,
    args: &[&str],
    event_tx: &mpsc::Sender<AppEvent>,
) {
    if args.is_empty() {
        app.log(
            LogKind::Error,
            "Usage: sub <subject> [group1,group2]".into(),
        );
        return;
    }
    let dot_subject = args[0];
    let subject = parse_subject(dot_subject);
    let groups = parse_groups(args.get(1).copied());
    let groups_owned: Vec<String> = groups.iter().map(|s| s.to_string()).collect();

    let id = app.next_sub_id();
    let label = if groups.is_empty() {
        format!("{dot_subject} (fanout)")
    } else {
        format!("{dot_subject} QG[{}]", groups.join(","))
    };
    app.subscriptions.insert(id, label.clone());
    app.log(LogKind::System, format!("sub#{id} listening on {label}"));

    let c = client.clone();
    let tx = event_tx.clone();

    let handle = tokio::spawn(async move {
        let groups_ref: Vec<&str> = groups_owned.iter().map(|s| s.as_str()).collect();
        let mut stream = match c.subscribe_raw(&subject, &groups_ref).await {
            Ok(s) => s,
            Err(e) => {
                let _ = tx
                    .send(AppEvent::Error {
                        sub_id: id,
                        error: e.to_string(),
                    })
                    .await;
                return;
            }
        };

        while let Some(result) = stream.next().await {
            match result {
                Ok(env) => {
                    let subject_display = Subject::from_bytes(&env.subject)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| format!("<{} bytes>", env.subject.len()));
                    let payload_str = String::from_utf8(env.payload.clone())
                        .unwrap_or_else(|_| format!("<{} bytes>", env.payload.len()));
                    let _ = tx
                        .send(AppEvent::MessageReceived {
                            sub_id: id,
                            subject: subject_display,
                            payload: payload_str,
                        })
                        .await;
                }
                Err(e) => {
                    let _ = tx
                        .send(AppEvent::Error {
                            sub_id: id,
                            error: e.to_string(),
                        })
                        .await;
                }
            }
        }
        let _ = tx.send(AppEvent::SubscriptionEnded(id)).await;
    });
    app.sub_handles.insert(id, handle);
}

async fn cmd_pub(client: &HermesClient, app: &mut App, args: &[&str]) {
    if args.len() < 2 {
        app.log(LogKind::Error, "Usage: pub <subject> <payload...>".into());
        return;
    }
    let dot_subject = args[0];
    let subject = Subject::from(dot_subject);
    let payload_str = args[1..].join(" ");
    let payload = payload_str.as_bytes().to_vec();

    match client.publish_raw(&subject, payload).await {
        Ok(()) => app.log(LogKind::Published, format!("[{dot_subject}] {payload_str}")),
        Err(e) => app.log(LogKind::Error, format!("publish failed: {e}")),
    }
}

async fn cmd_chat(client: &HermesClient, app: &mut App, args: &[&str]) {
    if args.len() < 2 {
        app.log(LogKind::Error, "Usage: chat <user> <text...>".into());
        return;
    }
    let user = args[0].to_string();
    let text = args[1..].join(" ");
    match client
        .publish(&ChatMessage {
            user: user.clone(),
            text: text.clone(),
        })
        .await
    {
        Ok(()) => app.log(LogKind::Published, format!("ChatMessage [{user}]: {text}")),
        Err(e) => app.log(LogKind::Error, format!("publish failed: {e}")),
    }
}

async fn cmd_order(client: &HermesClient, app: &mut App, args: &[&str]) {
    if args.len() < 2 {
        app.log(LogKind::Error, "Usage: order <id> <total>".into());
        return;
    }
    let order_id = args[0].to_string();
    let total: f64 = match args[1].parse() {
        Ok(v) => v,
        Err(_) => {
            app.log(LogKind::Error, "total must be a number".into());
            return;
        }
    };
    match client
        .publish(&OrderPlaced {
            order_id: order_id.clone(),
            total,
        })
        .await
    {
        Ok(()) => app.log(
            LogKind::Published,
            format!("OrderPlaced id={order_id} total={total}"),
        ),
        Err(e) => app.log(LogKind::Error, format!("publish failed: {e}")),
    }
}

async fn cmd_ship(client: &HermesClient, app: &mut App, args: &[&str]) {
    if args.len() < 2 {
        app.log(LogKind::Error, "Usage: ship <id> <carrier...>".into());
        return;
    }
    let order_id = args[0].to_string();
    let carrier = args[1..].join(" ");
    match client
        .publish(&OrderShipped {
            order_id: order_id.clone(),
            carrier: carrier.clone(),
        })
        .await
    {
        Ok(()) => app.log(
            LogKind::Published,
            format!("OrderShipped id={order_id} carrier={carrier}"),
        ),
        Err(e) => app.log(LogKind::Error, format!("publish failed: {e}")),
    }
}

async fn cmd_task(client: &HermesClient, app: &mut App, args: &[&str]) {
    if args.len() < 2 {
        app.log(LogKind::Error, "Usage: task <id> <payload...>".into());
        return;
    }
    let id: u32 = match args[0].parse() {
        Ok(v) => v,
        Err(_) => {
            app.log(LogKind::Error, "id must be a number".into());
            return;
        }
    };
    let payload = args[1..].join(" ");
    match client
        .publish(&Task {
            id,
            payload: payload.clone(),
        })
        .await
    {
        Ok(()) => app.log(
            LogKind::Published,
            format!("Task id={id} payload={payload}"),
        ),
        Err(e) => app.log(LogKind::Error, format!("publish failed: {e}")),
    }
}

async fn cmd_batch(client: &HermesClient, app: &mut App, args: &[&str]) {
    let n: usize = args.first().and_then(|s| s.parse().ok()).unwrap_or(10);
    let batch = client.batch_publisher();
    for i in 0..n {
        if let Err(e) = batch
            .send(&ChatMessage {
                user: "batch-bot".into(),
                text: format!("msg-{i}"),
            })
            .await
        {
            app.log(LogKind::Error, format!("batch send failed: {e}"));
            return;
        }
    }
    match batch.flush().await {
        Ok(ack) => app.log(
            LogKind::Published,
            format!("batch of {n} published (accepted={})", ack.accepted),
        ),
        Err(e) => app.log(LogKind::Error, format!("batch flush failed: {e}")),
    }
}

async fn cmd_dpub(client: &HermesClient, app: &mut App, args: &[&str]) {
    if args.len() < 2 {
        app.log(LogKind::Error, "Usage: dpub <subject> <payload...>".into());
        return;
    }
    let dot_subject = args[0];
    let subject = Subject::from(dot_subject);
    let payload_str = args[1..].join(" ");
    let payload = payload_str.as_bytes().to_vec();

    // Use the durable publish gRPC call (not fire-and-forget publish_raw)
    use hermes_proto::EventEnvelope;
    use hermes_proto::broker_client::BrokerClient;
    use tokio_stream::once;

    let envelope = EventEnvelope {
        id: uuid::Uuid::now_v7().to_string(),
        subject: subject.to_bytes(),
        payload,
        headers: HashMap::new(),
        timestamp_nanos: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
    };

    let uri = client.uri().to_string();
    match BrokerClient::connect(uri).await {
        Ok(mut grpc) => match grpc.publish_durable(once(envelope)).await {
            Ok(resp) => {
                let ack = resp.into_inner();
                app.log(
                    LogKind::Published,
                    format!(
                        "[durable] [{dot_subject}] {payload_str} (accepted={})",
                        ack.accepted
                    ),
                );
            }
            Err(e) => app.log(LogKind::Error, format!("dpub failed: {e}")),
        },
        Err(e) => app.log(LogKind::Error, format!("dpub connect failed: {e}")),
    }
}

async fn cmd_dsub(
    client: &HermesClient,
    app: &mut App,
    args: &[&str],
    event_tx: &mpsc::Sender<AppEvent>,
    auto_ack: bool,
) {
    if args.len() < 2 {
        let cmd_name = if auto_ack { "dsub" } else { "dsub-manual" };
        app.log(
            LogKind::Error,
            format!("Usage: {cmd_name} <consumer> <subject> [groups]"),
        );
        return;
    }
    let consumer_name = args[0].to_string();
    let dot_subject = args[1];
    let subject = parse_subject(dot_subject);
    let groups = parse_groups(args.get(2).copied());

    use hermes_proto::{
        DurableClientMessage, DurableSubscribeRequest, broker_client::BrokerClient,
        durable_client_message::Msg as ClientMsg, durable_server_message::Msg as ServerMsg,
    };
    use tokio_stream::wrappers::ReceiverStream;

    let id = app.next_sub_id();
    let mode = if auto_ack { "auto-ack" } else { "manual" };
    let label = format!("durable:{consumer_name} on {dot_subject} ({mode})");
    app.subscriptions.insert(id, label.clone());
    app.log(
        LogKind::System,
        format!("dsub#{id} '{consumer_name}' listening ({mode})"),
    );

    let uri = client.uri().to_string();
    let groups_owned: Vec<String> = groups.iter().map(|s| s.to_string()).collect();
    let tx = event_tx.clone();
    let subject_bytes = subject.to_bytes();
    let name = consumer_name.clone();

    let handle = tokio::spawn(async move {
        let mut grpc = match BrokerClient::connect(uri).await {
            Ok(c) => c,
            Err(e) => {
                let _ = tx
                    .send(AppEvent::Error {
                        sub_id: id,
                        error: format!("connect: {e}"),
                    })
                    .await;
                return;
            }
        };

        let (msg_tx, rx) = tokio::sync::mpsc::channel::<DurableClientMessage>(32);

        if msg_tx
            .send(DurableClientMessage {
                msg: Some(ClientMsg::Subscribe(DurableSubscribeRequest {
                    subject: subject_bytes,
                    consumer_name: name.clone(),
                    queue_groups: groups_owned,
                    max_in_flight: 10,
                    ack_timeout_seconds: 10,
                })),
            })
            .await
            .is_err()
        {
            let _ = tx
                .send(AppEvent::Error {
                    sub_id: id,
                    error: "channel closed".into(),
                })
                .await;
            return;
        }

        let response = match grpc.subscribe_durable(ReceiverStream::new(rx)).await {
            Ok(r) => r,
            Err(e) => {
                let _ = tx
                    .send(AppEvent::Error {
                        sub_id: id,
                        error: format!("dsub: {e}"),
                    })
                    .await;
                return;
            }
        };

        let mut server_stream = response.into_inner();
        while let Ok(Some(msg)) = server_stream.message().await {
            let (envelope, attempt) = match msg.msg {
                Some(ServerMsg::Envelope(env)) => (env, 1u32),
                Some(ServerMsg::Redelivery(r)) => match r.envelope {
                    Some(env) => (env, r.attempt),
                    None => continue,
                },
                None => continue,
            };

            let subject_display = Subject::from_bytes(&envelope.subject)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| format!("<{} bytes>", envelope.subject.len()));
            let payload_str = String::from_utf8(envelope.payload.clone())
                .unwrap_or_else(|_| format!("<{} bytes>", envelope.payload.len()));
            let msg_id = envelope.id.clone();

            let ack_sender = if auto_ack { None } else { Some(msg_tx.clone()) };

            let _ = tx
                .send(AppEvent::DurableReceived {
                    sub_id: id,
                    subject: subject_display,
                    payload: payload_str,
                    msg_id: msg_id.clone(),
                    attempt,
                    auto_ack,
                    ack_sender,
                })
                .await;

            if auto_ack {
                use hermes_proto::Ack;
                let _ = msg_tx
                    .send(DurableClientMessage {
                        msg: Some(ClientMsg::Ack(Ack { message_id: msg_id })),
                    })
                    .await;
            }
        }
        let _ = tx.send(AppEvent::SubscriptionEnded(id)).await;
    });
    app.sub_handles.insert(id, handle);
}

async fn cmd_ack(app: &mut App, args: &[&str]) {
    if args.is_empty() {
        app.log(LogKind::Error, "Usage: ack <msg_id>".into());
        return;
    }
    let msg_id = args[0];
    // Allow prefix matching for short IDs
    let full_id = find_pending_id(&app.pending_acks, msg_id);
    match full_id {
        Some(id) => {
            if let Some(tx) = app.pending_acks.remove(&id) {
                use hermes_proto::{Ack, DurableClientMessage, durable_client_message::Msg};
                let _ = tx
                    .send(DurableClientMessage {
                        msg: Some(Msg::Ack(Ack {
                            message_id: id.clone(),
                        })),
                    })
                    .await;
                app.log(LogKind::System, format!("acked {}", &id[..8.min(id.len())]));
            }
        }
        None => app.log(
            LogKind::Error,
            format!("no pending message matching '{msg_id}'"),
        ),
    }
}

async fn cmd_nack(app: &mut App, args: &[&str]) {
    if args.is_empty() {
        app.log(LogKind::Error, "Usage: nack <msg_id> [requeue=true]".into());
        return;
    }
    let msg_id = args[0];
    let requeue = args.get(1).map(|s| *s != "false").unwrap_or(true);

    let full_id = find_pending_id(&app.pending_acks, msg_id);
    match full_id {
        Some(id) => {
            if let Some(tx) = app.pending_acks.remove(&id) {
                use hermes_proto::{DurableClientMessage, Nack, durable_client_message::Msg};
                let _ = tx
                    .send(DurableClientMessage {
                        msg: Some(Msg::Nack(Nack {
                            message_id: id.clone(),
                            requeue,
                        })),
                    })
                    .await;
                let action = if requeue { "nacked+requeue" } else { "nacked" };
                app.log(
                    LogKind::System,
                    format!("{action} {}", &id[..8.min(id.len())]),
                );
            }
        }
        None => app.log(
            LogKind::Error,
            format!("no pending message matching '{msg_id}'"),
        ),
    }
}

fn find_pending_id(
    pending: &HashMap<String, mpsc::Sender<hermes_proto::DurableClientMessage>>,
    prefix: &str,
) -> Option<String> {
    // Exact match first
    if pending.contains_key(prefix) {
        return Some(prefix.to_string());
    }
    // Prefix match
    let matches: Vec<&String> = pending.keys().filter(|k| k.starts_with(prefix)).collect();
    if matches.len() == 1 {
        Some(matches[0].clone())
    } else {
        None
    }
}

fn cmd_unsub(app: &mut App, args: &[&str]) {
    if args.is_empty() {
        app.log(LogKind::Error, "Usage: unsub <id>".into());
        return;
    }
    let id: usize = match args[0].parse() {
        Ok(v) => v,
        Err(_) => {
            app.log(LogKind::Error, "id must be a number".into());
            return;
        }
    };
    if let Some(handle) = app.sub_handles.remove(&id) {
        handle.abort();
        app.subscriptions.remove(&id);
        app.log(LogKind::System, format!("sub#{id} cancelled"));
    } else {
        app.log(LogKind::Error, format!("no subscription #{id}"));
    }
}

fn cmd_subs(app: &mut App) {
    if app.subscriptions.is_empty() {
        app.log(LogKind::System, "no active subscriptions".into());
        return;
    }
    let mut entries: Vec<_> = app
        .subscriptions
        .iter()
        .map(|(id, desc)| (*id, desc.clone()))
        .collect();
    entries.sort_by_key(|(id, _)| *id);
    for (id, desc) in entries {
        app.log(LogKind::System, format!("  #{id} {desc}"));
    }
}

async fn cmd_flood(client: &HermesClient, app: &mut App, args: &[&str]) {
    if args.len() < 2 {
        app.log(
            LogKind::Error,
            "Usage: flood <subject> <count> [payload_size]".into(),
        );
        return;
    }
    let dot_subject = args[0];
    let subject = Subject::from(dot_subject);
    let count: usize = match args[1].parse() {
        Ok(v) => v,
        Err(_) => {
            app.log(LogKind::Error, "count must be a number".into());
            return;
        }
    };
    let payload_size: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(64);
    let payload = vec![b'x'; payload_size];

    let start = std::time::Instant::now();
    let mut errors = 0usize;
    for _ in 0..count {
        if client.publish_raw(&subject, payload.clone()).await.is_err() {
            errors += 1;
        }
    }
    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        count as f64 / elapsed.as_secs_f64()
    } else {
        count as f64
    };

    app.log(
        LogKind::Published,
        format!(
            "flood [{dot_subject}] {count} msgs ({payload_size}B each) in {:.2?} ({:.0} msg/s, {errors} errors)",
            elapsed, rate
        ),
    );
}

fn cmd_help(app: &mut App) {
    let lines = [
        "--- PUBLISH ---",
        "  pub <subject> <data>        publish raw payload",
        "  dpub <subject> <data>       durable publish (persisted)",
        "  chat <user> <text>          typed ChatMessage",
        "  order <id> <total>          typed OrderPlaced",
        "  ship <id> <carrier>         typed OrderShipped",
        "  task <id> <payload>         typed Task",
        "  batch <n>                   batch n ChatMessages",
        "  flood <subj> <n> [size]     stress-test: n raw msgs",
        "--- SUBSCRIBE ---",
        "  sub <subject> [grps]        subscribe (fanout/QG)",
        "  dsub <cons> <subj> [grps]   durable sub (auto-ack)",
        "  dsub-manual <c> <s> [grps]  durable sub (manual ack)",
        "  unsub <id>                  cancel subscription by ID",
        "  subs                        list active subscriptions",
        "--- DURABLE ACK/NACK ---",
        "  ack <msg_id>                ack a pending message",
        "  nack <msg_id> [requeue]     nack (default requeue=true)",
        "--- OTHER ---",
        "  info                        broker connection info",
        "  clear                       clear message log",
        "  quit / exit / q             quit",
        "--- SUBJECTS ---",
        "  Dot-notation: chat.room1, orders.*, logs.>",
        "  * = one segment, > = rest (like NATS)",
        "  Groups: sub orders.* workers,loggers",
    ];
    for line in lines {
        app.log(LogKind::System, line.to_string());
    }
}

fn cmd_info(app: &mut App) {
    let mode = if app.embedded { "embedded" } else { "external" };
    let store = if app.durable_enabled {
        "durable enabled"
    } else {
        "fire-and-forget only"
    };
    app.log(
        LogKind::System,
        format!("broker: {} ({mode}, {store})", app.broker_uri),
    );
    app.log(
        LogKind::System,
        format!(
            "subscriptions: {}, pending acks: {}",
            app.subscriptions.len(),
            app.pending_acks.len()
        ),
    );
}

// ---------------------------------------------------------------------------
// UI rendering
// ---------------------------------------------------------------------------

fn draw(frame: &mut ratatui::Frame, app: &App) {
    let outer = frame.area();

    // Main layout: body + input + status bar
    let vertical = Layout::vertical([
        Constraint::Min(5),    // body
        Constraint::Length(3), // input
        Constraint::Length(1), // status bar
    ]);
    let [body_area, input_area, status_area] = vertical.areas(outer);

    // Body: sidebar + messages
    let horizontal = Layout::horizontal([
        Constraint::Length(30), // sidebar (wider for new labels)
        Constraint::Min(30),    // messages
    ]);
    let [sidebar_area, messages_area] = horizontal.areas(body_area);

    // -- Sidebar: subscriptions --
    let sub_block = Block::default()
        .borders(Borders::ALL)
        .title(" Subscriptions ")
        .border_style(Style::default().fg(Color::Cyan));

    let sub_lines: Vec<Line> = if app.subscriptions.is_empty() {
        vec![Line::from(Span::styled(
            "  (none)",
            Style::default().fg(Color::DarkGray),
        ))]
    } else {
        let mut entries: Vec<_> = app.subscriptions.iter().collect();
        entries.sort_by_key(|(id, _)| *id);
        entries
            .iter()
            .map(|(id, desc)| {
                Line::from(vec![
                    Span::styled(format!("#{id} "), Style::default().fg(Color::Yellow)),
                    Span::raw(desc.as_str()),
                ])
            })
            .collect()
    };

    let sub_widget = Paragraph::new(sub_lines)
        .block(sub_block)
        .wrap(Wrap { trim: false });
    frame.render_widget(sub_widget, sidebar_area);

    // -- Messages log --
    let msg_block = Block::default()
        .borders(Borders::ALL)
        .title(" Messages ")
        .border_style(Style::default().fg(Color::Cyan));

    let inner_height = messages_area.height.saturating_sub(2) as usize;
    let total = app.messages.len();
    let start = if total > inner_height {
        app.scroll_offset.min(total.saturating_sub(inner_height))
    } else {
        0
    };
    let visible = &app.messages[start..total.min(start + inner_height)];

    let msg_lines: Vec<Line> = visible
        .iter()
        .map(|entry| {
            let (prefix, color) = match entry.kind {
                LogKind::System => ("SYS", Color::Cyan),
                LogKind::Published => ("PUB", Color::Green),
                LogKind::Received => ("RCV", Color::Magenta),
                LogKind::Error => ("ERR", Color::Red),
            };
            Line::from(vec![
                Span::styled(
                    format!(" {prefix} "),
                    Style::default().fg(Color::Black).bg(color),
                ),
                Span::raw(" "),
                Span::styled(&entry.text, Style::default().fg(color)),
            ])
        })
        .collect();

    let msg_paragraph = Paragraph::new(msg_lines)
        .block(msg_block)
        .wrap(Wrap { trim: false });
    frame.render_widget(msg_paragraph, messages_area);

    // -- Input line --
    let input_block = Block::default()
        .borders(Borders::ALL)
        .title(" Command ")
        .border_style(Style::default().fg(Color::Yellow));

    let input_text = Line::from(vec![
        Span::styled(
            "hermes> ",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(&app.input),
    ]);
    let input_widget = Paragraph::new(input_text).block(input_block);
    frame.render_widget(input_widget, input_area);

    // Place cursor
    let cursor_x = input_area.x + 1 + 8 + app.cursor_pos as u16;
    let cursor_y = input_area.y + 1;
    frame.set_cursor_position((cursor_x, cursor_y));

    // -- Status bar --
    let mode_label = if app.embedded { "embedded" } else { "external" };
    let status = Line::from(vec![
        Span::styled(
            " help",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" Commands  "),
        Span::styled(
            "Up/Down",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" History  "),
        Span::styled(
            "Ctrl-C",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" Quit  "),
        Span::styled(
            "PgUp/PgDn",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" Scroll  "),
        Span::styled(
            format!(
                " {} subs  {} msgs  {} pending  {mode_label}",
                app.subscriptions.len(),
                app.messages.len(),
                app.pending_acks.len(),
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    frame.render_widget(Paragraph::new(status), status_area);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let (broker_uri, embedded, durable_enabled) = if let Some(ref url) = cli.connect {
        (url.clone(), false, true) // assume external broker may support durable
    } else {
        let durable = !cli.no_store;
        let addr = start_broker(&cli).await;
        (format!("http://{addr}"), true, durable)
    };

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(ratatui::backend::CrosstermBackend::new(stdout()))?;

    let client = HermesClient::connect(&broker_uri).await?;
    let mut app = App::new(broker_uri.clone(), embedded, durable_enabled);

    let mode_str = if embedded { "embedded" } else { "external" };
    let store_str = if durable_enabled {
        "fire-and-forget + durable"
    } else {
        "fire-and-forget only"
    };
    app.log(
        LogKind::System,
        format!("Connected to {broker_uri} ({mode_str}, {store_str})"),
    );
    app.log(
        LogKind::System,
        "Type 'help' for commands. Up/Down for history.".into(),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(256);

    // Keyboard events from blocking thread
    let (key_tx, mut key_rx) = mpsc::channel::<Event>(64);
    std::thread::spawn(move || {
        loop {
            if event::poll(Duration::from_millis(50)).unwrap_or(false)
                && let Ok(ev) = event::read()
                && key_tx.blocking_send(ev).is_err()
            {
                break;
            }
        }
    });

    loop {
        terminal.draw(|f| draw(f, &app))?;

        tokio::select! {
            key_event = key_rx.recv() => {
                let Some(ev) = key_event else { break };
                match ev {
                    Event::Key(KeyEvent { code, modifiers, .. }) => {
                        match (code, modifiers) {
                            (KeyCode::Char('c'), KeyModifiers::CONTROL) => break,

                            (KeyCode::Enter, _) => {
                                let input = app.input.clone();
                                app.input.clear();
                                app.cursor_pos = 0;
                                app.history_index = None;
                                if !input.trim().is_empty() {
                                    app.history.push(input.trim().to_string());
                                    execute_command(input.trim(), &client, &mut app, &event_tx).await;
                                    if app.should_quit {
                                        break;
                                    }
                                }
                            }

                            (KeyCode::Up, _) => {
                                if !app.history.is_empty() {
                                    let idx = match app.history_index {
                                        None => {
                                            app.saved_input = app.input.clone();
                                            app.history.len() - 1
                                        }
                                        Some(i) => i.saturating_sub(1),
                                    };
                                    app.history_index = Some(idx);
                                    app.input = app.history[idx].clone();
                                    app.cursor_pos = app.input_char_count();
                                }
                            }
                            (KeyCode::Down, _) => {
                                if let Some(idx) = app.history_index {
                                    if idx + 1 < app.history.len() {
                                        let new_idx = idx + 1;
                                        app.history_index = Some(new_idx);
                                        app.input = app.history[new_idx].clone();
                                        app.cursor_pos = app.input_char_count();
                                    } else {
                                        app.history_index = None;
                                        app.input = app.saved_input.clone();
                                        app.cursor_pos = app.input_char_count();
                                    }
                                }
                            }

                            (KeyCode::Char(c), _) => {
                                let byte_pos = app.cursor_byte_pos();
                                app.input.insert(byte_pos, c);
                                app.cursor_pos += 1;
                            }
                            (KeyCode::Backspace, _) => {
                                if app.cursor_pos > 0 {
                                    app.cursor_pos -= 1;
                                    let byte_pos = app.cursor_byte_pos();
                                    app.input.remove(byte_pos);
                                }
                            }
                            (KeyCode::Delete, _) => {
                                if app.cursor_pos < app.input_char_count() {
                                    let byte_pos = app.cursor_byte_pos();
                                    app.input.remove(byte_pos);
                                }
                            }
                            (KeyCode::Left, _) => {
                                app.cursor_pos = app.cursor_pos.saturating_sub(1);
                            }
                            (KeyCode::Right, _) => {
                                if app.cursor_pos < app.input_char_count() {
                                    app.cursor_pos += 1;
                                }
                            }
                            (KeyCode::Home, _) => app.cursor_pos = 0,
                            (KeyCode::End, _) => app.cursor_pos = app.input_char_count(),

                            (KeyCode::PageUp, _) => {
                                app.scroll_offset = app.scroll_offset.saturating_sub(10);
                            }
                            (KeyCode::PageDown, _) => {
                                app.scroll_offset = app.scroll_offset.saturating_add(10);
                                app.scroll_offset = app.scroll_offset
                                    .min(app.messages.len().saturating_sub(1));
                            }

                            _ => {}
                        }
                    }
                    Event::Resize(_, _) => {}
                    _ => {}
                }
            }

            app_event = event_rx.recv() => {
                let Some(ev) = app_event else { continue };
                match ev {
                    AppEvent::MessageReceived { sub_id, subject, payload } => {
                        app.log(
                            LogKind::Received,
                            format!("sub#{sub_id} [{subject}] {payload}"),
                        );
                    }
                    AppEvent::DurableReceived { sub_id, subject, payload, msg_id, attempt, auto_ack, ack_sender } => {
                        let short_id = &msg_id[..8.min(msg_id.len())];
                        if auto_ack {
                            app.log(
                                LogKind::Received,
                                format!(
                                    "dsub#{sub_id} [{subject}] {payload} (id={short_id}, attempt={attempt}, auto-acked)",
                                ),
                            );
                        } else {
                            app.log(
                                LogKind::Received,
                                format!(
                                    "dsub#{sub_id} [{subject}] {payload} (id={short_id}, attempt={attempt}, PENDING — use ack/nack {short_id})",
                                ),
                            );
                            if let Some(sender) = ack_sender {
                                app.pending_acks.insert(msg_id, sender);
                            }
                        }
                    }
                    AppEvent::SubscriptionEnded(id) => {
                        app.subscriptions.remove(&id);
                        app.sub_handles.remove(&id);
                        app.log(LogKind::System, format!("sub#{id} stream ended"));
                    }
                    AppEvent::Error { sub_id, error } => {
                        app.log(LogKind::Error, format!("sub#{sub_id}: {error}"));
                    }
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(stdout(), LeaveAlternateScreen)?;
    println!("Bye!");
    Ok(())
}
