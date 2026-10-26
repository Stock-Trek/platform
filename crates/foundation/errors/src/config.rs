use crate::retryability::Retryability;
use std::error::Error as StdError;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ConfigError {
    #[error("invalid configuration: {0}")]
    Build(String),
    #[error("{name} must not be empty")]
    Empty { name: String },
    #[error("{name} is invalid: {message}")]
    Invalid {
        name: String,
        message: String,
        #[source]
        source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    },
    #[error("{name} is missing")]
    Missing { name: String },
    #[error("{name} is not valid UTF-8")]
    NotUnicode { name: String },
}

impl ConfigError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}
