//! Interactive TUI for the hermes.
//!
//! Starts an embedded broker (fire-and-forget + durable) and gives you a
//! full-screen terminal UI to publish, subscribe, ack, nack, etc.
//!
//! Run:
//!   cargo run -p hermes-integration-tests --example repl
//!
//! Press F1 for help inside the TUI.

use std::collections::HashMap;
use std::io::stdout;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{ExecutableCommand, execute};
use futures::StreamExt;
use hermes_client::HermesClient;
use hermes_core::{Event as BrokerEvent, Subject, event_group};
use ratatui::Terminal;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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
    scroll_offset: usize,
    show_help: bool,
    should_quit: bool,
    sub_counter: Arc<AtomicUsize>,
}

impl App {
    fn new() -> Self {
        Self {
            input: String::new(),
            cursor_pos: 0,
            messages: Vec::new(),
            subscriptions: HashMap::new(),
            scroll_offset: 0,
            show_help: false,
            should_quit: false,
            sub_counter: Arc::new(AtomicUsize::new(0)),
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

async fn start_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = hermes_server::config::ServerConfig::default();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    config.store_path = Some(tmp.path().to_path_buf());
    config.redelivery_interval_secs = 2;
    config.default_ack_timeout_secs = 10;
    tokio::spawn(async move {
        let _tmp = tmp;
        hermes_server::run_with_config(listener, config)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn subject_to_json(dot_subject: &str) -> String {
    Subject::from(dot_subject).to_json()
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
        "help" | "?" => app.show_help = !app.show_help,
        "sub" => cmd_sub(client, app, args, event_tx).await,
        "pub" => cmd_pub(client, app, args).await,
        "chat" => cmd_chat(client, app, args).await,
        "order" => cmd_order(client, app, args).await,
        "ship" => cmd_ship(client, app, args).await,
        "task" => cmd_task(client, app, args).await,
        "batch" => cmd_batch(client, app, args).await,
        "dpub" => cmd_dpub(client, app, args).await,
        "dsub" => cmd_dsub(client, app, args, event_tx).await,
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
    let subject_json = subject_to_json(dot_subject);
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
    let sj = subject_json.clone();

    tokio::spawn(async move {
        let groups_ref: Vec<&str> = groups_owned.iter().map(|s| s.as_str()).collect();
        let mut stream = match c.subscribe_raw(&sj, &groups_ref).await {
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
                    let subject_display = Subject::from_json(&env.subject)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| env.subject.clone());
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

    match client.publish_raw(&subject, payload).await {
        Ok(()) => app.log(
            LogKind::Published,
            format!("[durable] [{dot_subject}] {payload_str}"),
        ),
        Err(e) => app.log(LogKind::Error, format!("dpub failed: {e}")),
    }
}

async fn cmd_dsub(
    client: &HermesClient,
    app: &mut App,
    args: &[&str],
    event_tx: &mpsc::Sender<AppEvent>,
) {
    if args.len() < 2 {
        app.log(
            LogKind::Error,
            "Usage: dsub <consumer> <subject> [groups]".into(),
        );
        return;
    }
    let consumer_name = args[0].to_string();
    let dot_subject = args[1];
    let subject_json = subject_to_json(dot_subject);
    let groups = parse_groups(args.get(2).copied());

    use hermes_proto::{
        DurableClientMessage, DurableSubscribeRequest, broker_client::BrokerClient,
        durable_client_message::Msg as ClientMsg, durable_server_message::Msg as ServerMsg,
    };
    use tokio_stream::wrappers::ReceiverStream;

    let id = app.next_sub_id();
    let label = format!("durable:{consumer_name} on {dot_subject}");
    app.subscriptions.insert(id, label.clone());
    app.log(
        LogKind::System,
        format!("dsub#{id} '{consumer_name}' listening (auto-ack)"),
    );

    let uri = client.uri().to_string();
    let groups_owned: Vec<String> = groups.iter().map(|s| s.to_string()).collect();
    let tx = event_tx.clone();
    let sj = subject_json.clone();
    let name = consumer_name.clone();

    tokio::spawn(async move {
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
                    subject: sj,
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

            let subject_display = Subject::from_json(&envelope.subject)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| envelope.subject.clone());
            let payload_str = String::from_utf8(envelope.payload.clone())
                .unwrap_or_else(|_| format!("<{} bytes>", envelope.payload.len()));
            let msg_id = envelope.id.clone();

            let _ = tx
                .send(AppEvent::DurableReceived {
                    sub_id: id,
                    subject: subject_display,
                    payload: payload_str,
                    msg_id: msg_id.clone(),
                    attempt,
                })
                .await;

            // Auto-ack
            use hermes_proto::Ack;
            let _ = msg_tx
                .send(DurableClientMessage {
                    msg: Some(ClientMsg::Ack(Ack { message_id: msg_id })),
                })
                .await;
        }
        let _ = tx.send(AppEvent::SubscriptionEnded(id)).await;
    });
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
        Constraint::Length(26), // sidebar
        Constraint::Min(30),    // messages
    ]);
    let [sidebar_area, messages_area] = horizontal.areas(body_area);

    // -- Sidebar: subscriptions --
    let sub_items: Vec<ListItem> = {
        let mut entries: Vec<_> = app.subscriptions.iter().collect();
        entries.sort_by_key(|(id, _)| *id);
        entries
            .iter()
            .map(|(id, desc)| {
                ListItem::new(Line::from(vec![
                    Span::styled(format!("#{id} "), Style::default().fg(Color::Yellow)),
                    Span::raw(desc.as_str()),
                ]))
            })
            .collect()
    };

    let sub_block = Block::default()
        .borders(Borders::ALL)
        .title(" Subscriptions ")
        .border_style(Style::default().fg(Color::Cyan));

    let sub_list = if sub_items.is_empty() {
        List::new(vec![ListItem::new(Span::styled(
            "  (none)",
            Style::default().fg(Color::DarkGray),
        ))])
        .block(sub_block)
    } else {
        List::new(sub_items).block(sub_block)
    };
    frame.render_widget(sub_list, sidebar_area);

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

    let msg_paragraph = Paragraph::new(msg_lines).block(msg_block);
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
    let status = Line::from(vec![
        Span::styled(
            " F1",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" Help  "),
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
                " {} subs  {} msgs",
                app.subscriptions.len(),
                app.messages.len()
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    frame.render_widget(Paragraph::new(status), status_area);

    // -- Help overlay --
    if app.show_help {
        draw_help_popup(frame, outer);
    }
}

fn draw_help_popup(frame: &mut ratatui::Frame, area: Rect) {
    let popup_width = 60.min(area.width.saturating_sub(4));
    let popup_height = 22.min(area.height.saturating_sub(4));
    let x = (area.width.saturating_sub(popup_width)) / 2;
    let y = (area.height.saturating_sub(popup_height)) / 2;
    let popup_area = Rect::new(x, y, popup_width, popup_height);

    let clear = Paragraph::new("").block(Block::default().style(Style::default().bg(Color::Black)));
    frame.render_widget(clear, popup_area);

    let help_lines = vec![
        Line::from(Span::styled(
            "  COMMANDS",
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::raw(""),
        Line::from(vec![
            Span::styled(
                "  sub <subject> [grps]   ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("subscribe (fanout/QG)"),
        ]),
        Line::from(vec![
            Span::styled(
                "  pub <subject> <data>   ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("publish raw payload"),
        ]),
        Line::from(vec![
            Span::styled(
                "  chat <user> <text>     ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("typed ChatMessage"),
        ]),
        Line::from(vec![
            Span::styled(
                "  order <id> <total>     ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("typed OrderPlaced"),
        ]),
        Line::from(vec![
            Span::styled(
                "  ship <id> <carrier>    ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("typed OrderShipped"),
        ]),
        Line::from(vec![
            Span::styled(
                "  task <id> <payload>    ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("typed Task"),
        ]),
        Line::from(vec![
            Span::styled(
                "  batch <n>              ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("batch n ChatMessages"),
        ]),
        Line::from(vec![
            Span::styled(
                "  dpub <subj> <data>     ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("durable publish"),
        ]),
        Line::from(vec![
            Span::styled(
                "  dsub <cons> <subj>     ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("durable sub (auto-ack)"),
        ]),
        Line::from(vec![
            Span::styled(
                "  clear                  ",
                Style::default().fg(Color::Green),
            ),
            Span::raw("clear message log"),
        ]),
        Line::raw(""),
        Line::from(Span::styled(
            "  SUBJECTS",
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::raw("  Dot-notation: chat.room1, orders.*, logs.>"),
        Line::raw("  * = one segment, > = rest (like NATS)"),
        Line::raw("  Groups: sub orders.* workers,loggers"),
        Line::raw(""),
        Line::from(Span::styled(
            "  Press F1 or Esc to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let help = Paragraph::new(help_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Help ")
                .border_style(Style::default().fg(Color::Yellow)),
        )
        .wrap(Wrap { trim: false });

    frame.render_widget(help, popup_area);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = start_broker().await;

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(ratatui::backend::CrosstermBackend::new(stdout()))?;

    let client = HermesClient::connect(format!("http://{addr}")).await?;
    let mut app = App::new();
    app.log(
        LogKind::System,
        format!("Broker started on {addr} (fire-and-forget + durable)"),
    );
    app.log(
        LogKind::System,
        "Press F1 for help. Type commands below.".into(),
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
                        if app.show_help {
                            match code {
                                KeyCode::F(1) | KeyCode::Esc => app.show_help = false,
                                _ => {}
                            }
                            continue;
                        }

                        match (code, modifiers) {
                            (KeyCode::Char('c'), KeyModifiers::CONTROL) => break,
                            (KeyCode::F(1), _) => app.show_help = true,

                            (KeyCode::Enter, _) => {
                                let input = app.input.clone();
                                app.input.clear();
                                app.cursor_pos = 0;
                                if !input.trim().is_empty() {
                                    execute_command(input.trim(), &client, &mut app, &event_tx).await;
                                    if app.should_quit {
                                        break;
                                    }
                                }
                            }

                            (KeyCode::Char(c), _) => {
                                app.input.insert(app.cursor_pos, c);
                                app.cursor_pos += 1;
                            }
                            (KeyCode::Backspace, _) => {
                                if app.cursor_pos > 0 {
                                    app.cursor_pos -= 1;
                                    app.input.remove(app.cursor_pos);
                                }
                            }
                            (KeyCode::Delete, _) => {
                                if app.cursor_pos < app.input.len() {
                                    app.input.remove(app.cursor_pos);
                                }
                            }
                            (KeyCode::Left, _) => {
                                app.cursor_pos = app.cursor_pos.saturating_sub(1);
                            }
                            (KeyCode::Right, _) => {
                                if app.cursor_pos < app.input.len() {
                                    app.cursor_pos += 1;
                                }
                            }
                            (KeyCode::Home, _) => app.cursor_pos = 0,
                            (KeyCode::End, _) => app.cursor_pos = app.input.len(),

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
                    AppEvent::DurableReceived { sub_id, subject, payload, msg_id, attempt } => {
                        app.log(
                            LogKind::Received,
                            format!(
                                "dsub#{sub_id} [{subject}] {payload} (id={}, attempt={attempt}, auto-acked)",
                                &msg_id[..8.min(msg_id.len())]
                            ),
                        );
                    }
                    AppEvent::SubscriptionEnded(id) => {
                        app.subscriptions.remove(&id);
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
