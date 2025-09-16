use thiserror::Error;

/// Errors that can occur in the distributed key-value store
#[derive(Error, Debug)]
pub enum KvError {
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Transaction conflict: {0}")]
    TransactionConflict(String),

    #[error("Transaction aborted: {0}")]
    TransactionAborted(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Timeout error: {0}")]
    TimeoutError(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Cluster not available")]
    ClusterNotAvailable,

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> Self {
        KvError::SerializationError(err.to_string())
    }
}

impl From<bincode::Error> for KvError {
    fn from(err: bincode::Error) -> Self {
        KvError::SerializationError(err.to_string())
    }
}

impl From<std::io::Error> for KvError {
    fn from(err: std::io::Error) -> Self {
        KvError::StorageError(err.to_string())
    }
}

impl From<sled::Error> for KvError {
    fn from(err: sled::Error) -> Self {
        KvError::StorageError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, KvError>;