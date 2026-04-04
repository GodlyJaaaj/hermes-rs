//! Core routing engine for the Hermes message broker.
//!
//! This crate provides the single-threaded [`Router`](router::Router) task that owns
//! a trie-based subject index and a [`SlotMap`](slot::SlotMap) of subscriber channels.
//!
//! # Subject matching
//!
//! Subjects are dot-separated tokens (e.g. `orders.eu.created`).
//! Subscriptions support two wildcards:
//!
//! - `*` — matches exactly one token (`orders.*.created`)
//! - `>` — matches one or more trailing tokens (`orders.>`)
//!
//! # Delivery modes
//!
//! - **Fanout** — every subscriber receives every matching message (broadcast channel).
//! - **Queue group** — messages are distributed round-robin across group members (mpsc channels).

pub mod router;
pub mod slot;
pub mod trie;
