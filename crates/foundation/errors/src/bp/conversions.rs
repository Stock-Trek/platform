use super::BpError;
#[cfg(feature = "lambda")]
use crate::ErrorType;
#[cfg(feature = "http")]
use crate::http::HttpError;
use crate::io::IoError;
#[cfg(feature = "lambda")]
use crate::lambda::LambdaError;
use crate::sqs::SqsError;
use crate::system::SystemError;
use crate::timeout::TimeoutError;
#[cfg(feature = "lambda")]
use lambda_runtime::Diagnostic;
use platform_messages::sqs_message::SqsMessageError;
use platform_types::{new_types::ArithmeticError, protocol::ProtocolError};
use std::time::Duration;

#[cfg(feature = "lambda")]
impl From<BpError> for Diagnostic {
    fn from(value: BpError) -> Self {
        Diagnostic {
            error_type: value.error_type().into(),
            error_message: value.to_string(),
        }
    }
}

impl From<ArithmeticError> for BpError {
    fn from(error: ArithmeticError) -> Self {
        Self::System(SystemError::Arithmetic(error))
    }
}

impl From<std::io::Error> for BpError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(IoError::from(error))
    }
}

#[cfg(feature = "archive")]
impl From<zip::result::ZipError> for BpError {
    fn from(error: zip::result::ZipError) -> Self {
        Self::Io(IoError::from(error))
    }
}

#[cfg(feature = "archive")]
impl From<arrow_schema::ArrowError> for BpError {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self::Io(IoError::from(error))
    }
}

#[cfg(feature = "archive")]
impl From<parquet::errors::ParquetError> for BpError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Io(IoError::from(error))
    }
}

impl From<tokio::task::JoinError> for BpError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::System(SystemError::from(error))
    }
}

#[cfg(feature = "http")]
impl From<serde_json::Error> for BpError {
    fn from(error: serde_json::Error) -> Self {
        Self::Http(HttpError::from(error))
    }
}

#[cfg(not(feature = "http"))]
impl From<serde_json::Error> for BpError {
    fn from(error: serde_json::Error) -> Self {
        Self::System(SystemError::External(Box::new(error)))
    }
}

#[cfg(feature = "http")]
impl From<http::Error> for BpError {
    fn from(error: http::Error) -> Self {
        Self::Http(HttpError::from(error))
    }
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for BpError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(HttpError::from(error))
    }
}

impl From<tracing::subscriber::SetGlobalDefaultError> for BpError {
    fn from(error: tracing::subscriber::SetGlobalDefaultError) -> Self {
        Self::System(SystemError::from(error))
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for BpError {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::System(SystemError::External(error))
    }
}

#[cfg(feature = "lambda")]
impl From<aws_sdk_lambda::operation::invoke::InvokeError> for BpError {
    fn from(error: aws_sdk_lambda::operation::invoke::InvokeError) -> Self {
        Self::Lambda(LambdaError::from(error))
    }
}

impl From<Duration> for BpError {
    fn from(duration: Duration) -> Self {
        Self::Timeout(TimeoutError::BuildTimeout(duration))
    }
}

impl From<ProtocolError> for BpError {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::InvalidUrl {
                name,
                value,
                source,
            } => Self::bad_value(name, value, source.to_string()),
            ProtocolError::UnsupportedScheme {
                name,
                value,
                allowed,
                scheme,
            } => Self::bad_value(
                name,
                value,
                format!(
                    "URL scheme must be one of {}, got {scheme}",
                    allowed.join(", ")
                ),
            ),
        }
    }
}

impl From<SqsMessageError> for BpError {
    fn from(error: SqsMessageError) -> Self {
        match error {
            SqsMessageError::MessageTooLarge { size, limit } => {
                Self::Sqs(SqsError::MessageTooLarge { size, limit })
            }
            SqsMessageError::Serialize(error) => Self::from(error),
        }
    }
}
