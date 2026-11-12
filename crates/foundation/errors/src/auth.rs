use crate::Retryability;
use std::{error::Error as StdError, time::Duration};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum AuthError {
    #[error("no API token {token_id:?}")]
    UnknownToken { token_id: String },
    #[error("API token {token_id:?} already exists")]
    TokenExists { token_id: String },
    #[error("API token {token_id:?} was modified concurrently")]
    Conflict { token_id: String },
    #[error("invalid {field} for API token: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("secret does not match API token {token_id:?}")]
    InvalidSecret { token_id: String },
    #[error("API token {token_id:?} has been revoked")]
    Revoked { token_id: String },
    #[error("API token {token_id:?} has expired")]
    Expired { token_id: String },
    #[error("API token {token_id:?} is outside its rotation overlap")]
    RotationOverlapEnded { token_id: String },
    #[error("API token {token_id:?} is missing required scope {scope}")]
    MissingScope { token_id: String, scope: String },
    #[error("invalid JWT: {reason}")]
    InvalidJwt { reason: String },
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
            | Self::Conflict { .. }
            | Self::Unavailable { .. } => Retryability::Retryable,
            Self::UnknownToken { .. }
            | Self::TokenExists { .. }
            | Self::InvalidField { .. }
            | Self::InvalidSecret { .. }
            | Self::Revoked { .. }
            | Self::Expired { .. }
            | Self::RotationOverlapEnded { .. }
            | Self::MissingScope { .. }
            | Self::InvalidJwt { .. }
            | Self::Store {
                retryable: false, ..
            } => Retryability::Permanent,
        }
    }
}
