use crate::retryability::Retryability;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ValidationError {
    #[error("invalid path: {value:?}")]
    InvalidPath { value: String },
    #[error("Invalid value for {name}: {value}")]
    InvalidValue { name: String, value: String },
    #[error("Missing {0}")]
    MissingValue(String),
    #[error("Unsupported {name}: {value}")]
    Unsupported { name: String, value: String },
}

impl ValidationError {
    pub fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}
