use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connection failed: {0}")]
    Connect(#[from] tonic::transport::Error),

    #[error("rpc failed: {0}")]
    Rpc(#[from] tonic::Status),

    #[error("encode: {0}")]
    Encode(#[from] hermes_core::EncodeError),

    #[error("decode: {0}")]
    Decode(#[from] hermes_core::DecodeError),

    #[error("channel closed")]
    ChannelClosed,
}
