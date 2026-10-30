use crate::retryability::Retryability;

/// Errors raised by the shared NATS infrastructure.
#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum NatsError {
    #[error(
        "invalid KV bucket name {bucket:?}: must only contain alphanumeric characters, dashes, and underscores"
    )]
    InvalidBucket { bucket: String },
    #[error("NATS protocol error: {message}")]
    Protocol { message: String },
}

impl NatsError {
    pub fn invalid_bucket(bucket: impl Into<String>) -> Self {
        Self::InvalidBucket {
            bucket: bucket.into(),
        }
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Protocol { .. } => Retryability::Retryable,
            Self::InvalidBucket { .. } => Retryability::Permanent,
        }
    }
}
