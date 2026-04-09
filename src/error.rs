use arrow::error::ArrowError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RplError {
    /// Schema validation failure when connecting tasks.
    #[error("schema mismatch: task '{provider_task}' does not provide fields {missing_fields:?} required by task '{consumer_task}'")]
    SchemaMismatch {
        provider_task: String,
        consumer_task: String,
        missing_fields: Vec<String>,
    },
    /// Graph structure error (e.g. cycle detected, disconnected).
    #[error("graph error: {0}")]
    GraphError(String),
    /// Task execution failure.
    #[error("task '{task}' failed: {source}")]
    TaskError {
        task: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Arrow-level error.
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// I/O error (file staging, subprocess communication).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Transport error.
    #[error("transport error: {0}")]
    Transport(String),
    /// HyperQueue error.
    #[error("hyperqueue error: {0}")]
    Hq(String),
}

pub type Result<T> = std::result::Result<T, RplError>;
