use crate::retryability::Retryability;
use aws_credential_types::provider::error::CredentialsError;
use std::{error::Error as StdError, fmt};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum StorageError {
    #[error(
        "artifact {artifact:?} is {size} bytes, exceeding the maximum artifact size of {max_bytes} bytes"
    )]
    ArtifactTooLarge {
        artifact: String,
        size: usize,
        max_bytes: u64,
    },
    #[error(
        "source archive is {size} bytes, exceeding the maximum source archive size of {max_bytes} bytes"
    )]
    SourceArchiveTooLarge { size: u64, max_bytes: u64 },
    #[error(
        "extracted source is {size} bytes, exceeding the maximum extracted source size of {max_bytes} bytes"
    )]
    ExtractedSourceTooLarge { size: u64, max_bytes: u64 },
    #[error("failed to load AWS credentials: {0}")]
    Credentials(#[from] CredentialsError),
    #[error("invalid S3 endpoint URL: {source}")]
    InvalidEndpoint {
        #[source]
        source: url::ParseError,
    },
    #[error("S3 endpoint {endpoint:?} does not contain a host")]
    InvalidEndpointHost { endpoint: String },
    #[error("missing credentials")]
    MissingCredentials,
    #[error(
        "object {key:?} is {size} bytes, exceeding the maximum object size of {max_bytes} bytes"
    )]
    ObjectTooLarge {
        key: String,
        size: usize,
        max_bytes: u64,
    },
    #[error("S3 {operation} failed for {target}: {source}")]
    S3 {
        operation: &'static str,
        target: String,
        retryable: bool,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error(
        "S3 delete objects failed for {target}: {}",
        Self::format_delete_failures(.failures)
    )]
    S3DeleteObjects {
        target: String,
        failures: Vec<S3DeleteObjectFailure>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3DeleteObjectFailure {
    pub key: String,
    pub code: String,
    pub message: Option<String>,
}

impl S3DeleteObjectFailure {
    pub fn retryability(&self) -> Retryability {
        Retryability::from_s3_error_code(&self.code)
    }

    pub fn is_permanent(&self) -> bool {
        self.retryability().is_permanent()
    }
}

impl fmt::Display for S3DeleteObjectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(message) => write!(formatter, "{}: {} ({message})", self.key, self.code),
            None => write!(formatter, "{}: {}", self.key, self.code),
        }
    }
}

impl StorageError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::S3 {
                retryable: true, ..
            } => Retryability::Retryable,
            Self::S3DeleteObjects { failures, .. }
                if !failures.is_empty()
                    && failures.iter().all(S3DeleteObjectFailure::is_permanent) =>
            {
                Retryability::Permanent
            }
            Self::S3DeleteObjects { .. } => Retryability::Retryable,
            Self::S3 {
                retryable: false, ..
            }
            | Self::InvalidEndpoint { .. }
            | Self::InvalidEndpointHost { .. }
            | Self::MissingCredentials
            | Self::ObjectTooLarge { .. }
            | Self::ArtifactTooLarge { .. }
            | Self::SourceArchiveTooLarge { .. }
            | Self::ExtractedSourceTooLarge { .. } => Retryability::Permanent,
            Self::Credentials(error) => Retryability::from_credentials(error),
        }
    }

    fn format_delete_failures(failures: &[S3DeleteObjectFailure]) -> String {
        failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(key: &str, code: &str) -> S3DeleteObjectFailure {
        S3DeleteObjectFailure {
            key: key.to_string(),
            code: code.to_string(),
            message: None,
        }
    }

    #[test]
    fn delete_objects_with_only_permanent_failures_is_permanent() {
        let error = StorageError::S3DeleteObjects {
            target: "s3://bucket/2 objects".to_string(),
            failures: vec![
                failure("first", "AccessDenied"),
                failure("second", "NoSuchKey"),
            ],
        };

        assert_eq!(error.retryability(), Retryability::Permanent);
    }

    #[test]
    fn delete_objects_with_any_retryable_failure_is_retryable() {
        let error = StorageError::S3DeleteObjects {
            target: "s3://bucket/2 objects".to_string(),
            failures: vec![
                failure("first", "AccessDenied"),
                failure("second", "SlowDown"),
            ],
        };

        assert_eq!(error.retryability(), Retryability::Retryable);
    }

    #[test]
    fn delete_objects_with_only_retryable_failures_is_retryable() {
        let error = StorageError::S3DeleteObjects {
            target: "s3://bucket/1 objects".to_string(),
            failures: vec![failure("first", "InternalError")],
        };

        assert_eq!(error.retryability(), Retryability::Retryable);
    }

    #[test]
    fn delete_objects_display_includes_each_failure() {
        let error = StorageError::S3DeleteObjects {
            target: "s3://bucket/1 objects".to_string(),
            failures: vec![S3DeleteObjectFailure {
                key: "first".to_string(),
                code: "AccessDenied".to_string(),
                message: Some("denied".to_string()),
            }],
        };

        let message = error.to_string();
        assert!(message.contains("first: AccessDenied (denied)"));
    }
}
