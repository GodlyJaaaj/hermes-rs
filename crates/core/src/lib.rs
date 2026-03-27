mod error;
mod event;
mod group;
mod subject;

pub use error::{DecodeError, EncodeError};
pub use event::Event;
pub use group::EventGroup;
pub use subject::{Segment, Subject};

// Re-export the derive macro
pub use hermes_macros::Event;

/// Debug header set by the broker when a message is delivered through a queue group.
pub const DEBUG_QUEUE_GROUP_HEADER: &str = "x-hermes-debug-queue-group";

/// Encode an event to bytes using bincode (serde compat).
pub fn encode<E: serde::Serialize>(event: &E) -> Result<Vec<u8>, EncodeError> {
    bincode::serde::encode_to_vec(event, bincode::config::standard()).map_err(Into::into)
}

/// Decode bytes into an event using bincode (serde compat).
pub fn decode<E: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<E, DecodeError> {
    let (val, _len) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    Ok(val)
}

// Re-export serde for use in derive macros
pub use serde;
