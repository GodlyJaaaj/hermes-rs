//! Generated protobuf and gRPC definitions for the Hermes broker protocol.
//!
//! The gRPC service exposes two streaming RPCs:
//!
//! - **Publish** — client-streaming: fire-and-forget message publishing.
//! - **Subscribe** — bidirectional streaming: subscribe to subjects and receive deliveries.
//!
//! Re-exports all generated types at the crate root for convenience.

pub mod hermes {
    pub mod broker {
        pub mod v1 {
            tonic::include_proto!("hermes.broker.v1");
        }
    }
}

pub use hermes::broker::v1::*;

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("broker_descriptor");
