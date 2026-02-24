use thiserror::Error;

/// Error classification for retry policy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Connection timeout, lock contention, DuckDB busy
    Transient,
    /// Schema mismatch, table not found, permission denied
    Configuration,
    /// Disk full, replication slot limit exceeded
    Resource,
}

#[derive(Debug, Error)]
pub enum DuckPipeError {
    #[error("table not found: {schema}.{table}")]
    TableNotFound { schema: String, table: String },

    #[error("sync group not found: {name}")]
    GroupNotFound { name: String },

    #[error("invalid state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },

    #[error("spi error: {0}")]
    Spi(String),

    #[error("{0}")]
    Internal(String),
}

impl DuckPipeError {
    pub fn class(&self) -> ErrorClass {
        match self {
            DuckPipeError::TableNotFound { .. } | DuckPipeError::GroupNotFound { .. } => {
                ErrorClass::Configuration
            }
            DuckPipeError::InvalidStateTransition { .. } => ErrorClass::Configuration,
            DuckPipeError::Spi(_) => ErrorClass::Transient,
            DuckPipeError::Internal(_) => ErrorClass::Transient,
        }
    }
}
