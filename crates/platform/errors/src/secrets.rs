use crate::retryability::Retryability;
use std::time::Duration;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SecretsError {
    #[error("Refresh throttled")]
    RefreshThrottled,
    #[error("Secrets unavailable; retry after {retry_after:?}")]
    Unavailable { retry_after: Duration },
}

impl SecretsError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::RefreshThrottled => Retryability::Permanent,
            Self::Unavailable { .. } => Retryability::Retryable,
        }
    }
}
