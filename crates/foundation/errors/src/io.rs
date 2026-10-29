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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    fn command_failed(status: ExitStatus) -> IoError {
        IoError::CommandFailed {
            operation: "compile artifact",
            path: PathBuf::from("module.wasm"),
            status,
        }
    }

    #[test]
    fn permanent_io_error_kinds_are_not_retried() {
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::InvalidInput,
            ErrorKind::InvalidData,
            ErrorKind::Unsupported,
            ErrorKind::AlreadyExists,
            ErrorKind::FileTooLarge,
            ErrorKind::InvalidFilename,
            ErrorKind::TooManyLinks,
            ErrorKind::ArgumentListTooLong,
            ErrorKind::CrossesDevices,
            ErrorKind::NotSeekable,
            ErrorKind::UnexpectedEof,
        ] {
            assert_eq!(
                IoError::IoError(Error::from(kind)).retryability(),
                Retryability::Permanent,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn transient_io_error_kinds_are_retried() {
        for kind in [
            ErrorKind::Interrupted,
            ErrorKind::WouldBlock,
            ErrorKind::TimedOut,
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::NotConnected,
            ErrorKind::BrokenPipe,
            ErrorKind::StorageFull,
            ErrorKind::QuotaExceeded,
            ErrorKind::ResourceBusy,
            ErrorKind::Deadlock,
            ErrorKind::ExecutableFileBusy,
            ErrorKind::WriteZero,
            ErrorKind::OutOfMemory,
            ErrorKind::Other,
        ] {
            assert_eq!(
                IoError::IoError(Error::from(kind)).retryability(),
                Retryability::Retryable,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn wrapped_io_error_uses_the_source_error_kind() {
        let error = IoError::Io {
            operation: "read artifact",
            path: "artifact.wasm".to_string(),
            source: Box::new(Error::from(ErrorKind::NotFound)),
        };

        assert_eq!(error.retryability(), Retryability::Permanent);
    }

    #[cfg(unix)]
    mod unix {
        use super::*;
        use std::os::unix::process::ExitStatusExt;

        #[test]
        fn compile_failure_exit_code_is_permanent() {
            assert_eq!(
                command_failed(ExitStatus::from_raw(101 << 8)).retryability(),
                Retryability::Permanent
            );
        }

        #[test]
        fn process_killed_by_signal_is_retryable() {
            assert_eq!(
                command_failed(ExitStatus::from_raw(9)).retryability(),
                Retryability::Retryable
            );
        }

        #[test]
        fn interrupted_process_is_permanent() {
            assert_eq!(
                command_failed(ExitStatus::from_raw(2)).retryability(),
                Retryability::Permanent
            );
        }

        #[test]
        fn timeout_exit_code_is_retryable() {
            assert_eq!(
                command_failed(ExitStatus::from_raw(124 << 8)).retryability(),
                Retryability::Retryable
            );
        }
    }
}
