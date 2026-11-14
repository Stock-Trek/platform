use platform_errors::{ExchangeError, PlatformError};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    Acquire(String),
    Renew(String),
    Revoke(String),
    Bind(String),
    Sign(String),
    Transport(String),
    Expired,
}

impl AuthError {
    pub fn message(&self) -> &str {
        match self {
            Self::Acquire(message)
            | Self::Renew(message)
            | Self::Revoke(message)
            | Self::Bind(message)
            | Self::Sign(message)
            | Self::Transport(message) => message,
            Self::Expired => "credential expired",
        }
    }
}

impl fmt::Display for AuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acquire(message) => write!(formatter, "failed to acquire credential: {message}"),
            Self::Renew(message) => write!(formatter, "failed to renew credential: {message}"),
            Self::Revoke(message) => write!(formatter, "failed to revoke credential: {message}"),
            Self::Bind(message) => write!(formatter, "failed to bind credential: {message}"),
            Self::Sign(message) => write!(formatter, "failed to sign payload: {message}"),
            Self::Transport(message) => write!(formatter, "credential transport failed: {message}"),
            Self::Expired => write!(formatter, "credential expired"),
        }
    }
}

impl std::error::Error for AuthError {}

impl From<AuthError> for PlatformError {
    fn from(error: AuthError) -> Self {
        Self::Exchange(ExchangeError::BadResponse(error.to_string()))
    }
}
