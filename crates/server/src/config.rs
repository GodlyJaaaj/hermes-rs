use std::net::SocketAddr;
use std::path::PathBuf;

pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub subscriber_channel_capacity: usize,
    /// Path to the redb store file. None = fire-and-forget only, no durable mode.
    pub store_path: Option<PathBuf>,
    /// How often the redelivery loop runs (seconds).
    pub redelivery_interval_secs: u64,
    /// Max delivery attempts before dead-lettering.
    pub max_delivery_attempts: u32,
    /// How long to keep acked messages before GC (seconds).
    pub retention_secs: u64,
    /// Default ack timeout for durable subscriptions (seconds).
    pub default_ack_timeout_secs: u32,
    /// Default max in-flight messages for durable subscriptions.
    pub default_max_in_flight: u32,
    /// How often the GC loop runs (seconds).
    pub gc_interval_secs: u64,
    /// Max expired messages processed per consumer per redelivery cycle.
    pub redelivery_batch_size: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:4222".parse().unwrap(),
            subscriber_channel_capacity: 256,
            store_path: None,
            redelivery_interval_secs: 5,
            max_delivery_attempts: 5,
            retention_secs: 3600,
            default_ack_timeout_secs: 30,
            default_max_in_flight: 32,
            gc_interval_secs: 60,
            redelivery_batch_size: 100,
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(addr) = std::env::var("SCYLLA_LISTEN_ADDR") {
            if let Ok(parsed) = addr.parse() {
                config.listen_addr = parsed;
            }
        }
        if let Ok(cap) = std::env::var("SCYLLA_CHANNEL_CAPACITY") {
            if let Ok(parsed) = cap.parse() {
                config.subscriber_channel_capacity = parsed;
            }
        }
        if let Ok(path) = std::env::var("SCYLLA_STORE_PATH") {
            config.store_path = Some(PathBuf::from(path));
        }
        if let Ok(v) = std::env::var("SCYLLA_REDELIVERY_INTERVAL") {
            if let Ok(parsed) = v.parse() {
                config.redelivery_interval_secs = parsed;
            }
        }
        if let Ok(v) = std::env::var("SCYLLA_MAX_DELIVERY_ATTEMPTS") {
            if let Ok(parsed) = v.parse() {
                config.max_delivery_attempts = parsed;
            }
        }
        if let Ok(v) = std::env::var("SCYLLA_RETENTION_SECS") {
            if let Ok(parsed) = v.parse() {
                config.retention_secs = parsed;
            }
        }
        if let Ok(v) = std::env::var("SCYLLA_ACK_TIMEOUT") {
            if let Ok(parsed) = v.parse() {
                config.default_ack_timeout_secs = parsed;
            }
        }
        if let Ok(v) = std::env::var("SCYLLA_MAX_IN_FLIGHT") {
            if let Ok(parsed) = v.parse() {
                config.default_max_in_flight = parsed;
            }
        }
        if let Ok(v) = std::env::var("SCYLLA_GC_INTERVAL") {
            if let Ok(parsed) = v.parse() {
                config.gc_interval_secs = parsed;
            }
        }
        if let Ok(v) = std::env::var("SCYLLA_REDELIVERY_BATCH_SIZE") {
            if let Ok(parsed) = v.parse() {
                config.redelivery_batch_size = parsed;
            }
        }

        config
    }
}
