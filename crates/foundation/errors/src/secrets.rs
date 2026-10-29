use crate::retryability::Retryability;
use std::{error::Error as StdError, time::Duration};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SecretsError {
    #[error("secret {name:?} is missing from the secret store")]
    Missing { name: String },
    #[error("secret payload from {target:?} is invalid: {source}")]
    InvalidPayload {
        target: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("Refresh throttled")]
    RefreshThrottled,
    #[error("secret store {operation} failed for {target:?}: {source}")]
    Store {
        operation: &'static str,
        target: String,
        retryable: bool,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Secrets unavailable; retry after {retry_after:?}")]
    Unavailable { retry_after: Duration },
}

impl SecretsError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Store {
                retryable: true, ..
            }
            | Self::Unavailable { .. } => Retryability::Retryable,
            Self::Missing { .. }
            | Self::InvalidPayload { .. }
            | Self::RefreshThrottled
            | Self::Store {
                retryable: false, ..
            } => Retryability::Permanent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_invalid_payloads_are_permanent() {
        assert_eq!(
            SecretsError::Missing {
                name: "GITHUB_WEBHOOK_SECRET".into(),
            }
            .retryability(),
            Retryability::Permanent
        );

        let invalid = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        assert_eq!(
            SecretsError::InvalidPayload {
                target: "webhook".into(),
                source: invalid,
            }
            .retryability(),
            Retryability::Permanent
        );
    }

    #[test]
    fn store_failures_use_their_retryability() {
        let error = |retryable| SecretsError::Store {
            operation: "get_secret_value",
            target: "webhook".into(),
            retryable,
            source: "boom".into(),
        };

        assert_eq!(error(true).retryability(), Retryability::Retryable);
        assert_eq!(error(false).retryability(), Retryability::Permanent);
    }
}
