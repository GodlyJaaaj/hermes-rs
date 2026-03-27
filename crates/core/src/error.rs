use thiserror::Error;

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("bincode encode: {0}")]
    Bincode(#[from] bincode::error::EncodeError),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("bincode decode: {0}")]
    Bincode(#[from] bincode::error::DecodeError),

    #[error("unknown subject: {0}")]
    UnknownSubject(String),

    #[error("invalid subject JSON: {0}")]
    InvalidSubject(String),
}
