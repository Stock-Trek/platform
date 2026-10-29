use crate::retryability::Retryability;
use std::{error::Error as StdError, time::Duration};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SecretsError {
    #[error("secret path {path:?} returned no parameters; check the configured path")]
    Empty { path: String },
    #[error("{operation} failed for secret path {path}: {source}")]
    Load {
        operation: &'static str,
        path: String,
        retryable: bool,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Refresh throttled")]
    RefreshThrottled,
    #[error("Secrets unavailable; retry after {retry_after:?}")]
    Unavailable { retry_after: Duration },
}

impl SecretsError {
    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Load {
                retryable: true, ..
            }
            | Self::Unavailable { .. } => Retryability::Retryable,
            Self::Empty { .. }
            | Self::Load {
                retryable: false, ..
            }
            | Self::RefreshThrottled => Retryability::Permanent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_secret_path_is_permanent() {
        let error = SecretsError::Empty {
            path: "/platform/missing".to_string(),
        };

        assert_eq!(error.retryability(), Retryability::Permanent);
    }

    #[test]
    fn load_retryability_follows_the_ssm_error() {
        let error = |retryable| SecretsError::Load {
            operation: "SSM GetParametersByPath",
            path: "/platform/git-webhook/production".to_string(),
            retryable,
            source: "failure".into(),
        };

        assert_eq!(error(true).retryability(), Retryability::Retryable);
        assert_eq!(error(false).retryability(), Retryability::Permanent);
    }
}
