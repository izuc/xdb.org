//! Error types for xdb.

use std::io;
use thiserror::Error;

/// Convenience type alias for xdb results.
pub type Result<T> = std::result::Result<T, Error>;

/// All errors that xdb can produce.
#[derive(Error, Debug)]
pub enum Error {
    /// Underlying I/O error from the operating system.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Data on disk is corrupted or an internal invariant was violated.
    #[error("corruption: {0}")]
    Corruption(String),

    /// The requested key was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Caller supplied an invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The requested feature is not yet implemented.
    #[error("not supported: {0}")]
    NotSupported(String),

    /// Catch-all for unexpected internal failures.
    #[error("internal error: {0}")]
    Internal(String),

    /// The database is shutting down; no new operations accepted.
    #[error("database is shutting down")]
    ShutdownInProgress,
}

impl Error {
    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Error::InvalidArgument(msg.into())
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Error::NotFound(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }
}
