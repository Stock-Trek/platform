use crate::Retryability;
use std::{error::Error as StdError, time::Duration};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum AuthError {
    #[error("no API token {token_id:?}")]
    UnknownToken { token_id: String },
    #[error("API token {token_id:?} already exists")]
    TokenExists { token_id: String },
    #[error("invalid {field} for API token: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("token store {operation} failed for {target}: {source}")]
    Store {
        operation: &'static str,
        target: String,
        retryable: bool,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("token store unavailable; retry after {retry_after:?}")]
    Unavailable { retry_after: Duration },
}

impl AuthError {
    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Store {
                retryable: true, ..
            }
            | Self::Unavailable { .. } => Retryability::Retryable,
            Self::UnknownToken { .. }
            | Self::TokenExists { .. }
            | Self::InvalidField { .. }
            | Self::Store {
                retryable: false, ..
            } => Retryability::Permanent,
        }
    }
}
