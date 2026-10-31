use crate::retryability::Retryability;
use std::{error::Error as StdError, path::PathBuf, process::ExitStatus};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum IoError {
    #[cfg(feature = "archive")]
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("{operation} failed for {path} with {status}")]
    CommandFailed {
        operation: &'static str,
        path: PathBuf,
        status: ExitStatus,
    },
    #[error("I/O error while {operation} at {path}: {source}")]
    Io {
        operation: &'static str,
        path: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[cfg(feature = "archive")]
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[cfg(feature = "archive")]
    #[error("Zip error: {0}")]
    Zip(#[from] zip::result::ZipError),
}

impl IoError {
    pub fn retryability(&self) -> Retryability {
        match self {
            #[cfg(feature = "archive")]
            Self::Arrow(_) | Self::Parquet(_) => Retryability::Retryable,
            Self::CommandFailed { status, .. } => Retryability::from_exit_status(status),
            Self::Io { source, .. } => Retryability::from_io_source(source.as_ref()),
            Self::IoError(error) => Retryability::from_io_error(error),
            #[cfg(feature = "archive")]
            Self::Zip(_) => Retryability::Permanent,
        }
    }
}
