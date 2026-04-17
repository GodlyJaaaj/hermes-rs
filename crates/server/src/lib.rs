//! gRPC server for the Hermes message broker.
//!
//! Exposes [`BrokerService`](grpc::BrokerService), which bridges incoming gRPC streams
//! to the core [`Router`](hermes_broker::router::Router) via an mpsc command channel.

mod delivery_rx;
pub mod grpc;
