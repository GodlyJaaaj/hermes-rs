//! `hermes-client` provides a typed, async Rust client for the Hermes broker.
//!
//! It offers:
//! - fire-and-forget publishing/subscribing
//! - durable publishing/subscribing (at-least-once delivery)
//! - batch publishing for high-throughput producers
//! - raw envelope APIs for dynamic subjects
//!
//! ## Quick start
//!
//! ```no_run
//! use hermes_client::HermesClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = HermesClient::connect("http://127.0.0.1:4222").await?;
//!     println!("Connected to {}", client.uri());
//!     Ok(())
//! }
//! ```
//!
//! ## Main types
//!
//! - [`HermesClient`] — high-level entry point for publish/subscribe operations
//! - [`BatchPublisher`] — buffered, high-throughput publisher
//! - [`DurableSubscriber`] and [`DurableMessage`] — durable consumption + acknowledgements
//! - [`ClientError`] — error type for client operations

mod client;
mod durable_subscriber;
mod error;
mod publisher;
mod subscriber;

pub use client::HermesClient;
pub use durable_subscriber::{DurableMessage, DurableSubscriber};
pub use error::ClientError;
pub use publisher::BatchPublisher;
