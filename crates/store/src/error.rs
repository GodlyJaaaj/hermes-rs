use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("redb error: {0}")]
    Redb(#[from] redb::Error),

    #[error("redb database error: {0}")]
    Database(#[from] redb::DatabaseError),

    #[error("redb table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("redb transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("redb commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("redb storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("store not configured")]
    NotConfigured,

    #[error("message not found: {0}")]
    MessageNotFound(String),

    #[error("consumer not found: {0}")]
    ConsumerNotFound(String),

    #[error("invalid delivery state: {0}")]
    InvalidState(u8),
}
