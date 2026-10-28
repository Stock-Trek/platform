#[cfg(feature = "github")]
use crate::github::GitHubError;
#[cfg(feature = "http")]
use crate::http::HttpError;
#[cfg(feature = "lambda")]
use crate::lambda::LambdaError;
#[cfg(feature = "storage")]
use crate::storage::StorageError;
use crate::{
    binance::BinanceError, config::ConfigError, connector::ConnectorError, connector::SendFailure,
    data::DataError, exchange::ExchangeError, ingester::IngesterError, io::IoError,
    rate_limit::RateLimitError, retryability::Retryability, secrets::SecretsError, sqs::SqsError,
    system::SystemError, timeout::TimeoutError, validation::ValidationError,
};
#[cfg(feature = "lambda")]
use lambda_runtime::Diagnostic;
use platform_messages::sqs_message::SqsMessageError;
use platform_types::{new_types::ArithmeticError, protocol::ProtocolError};
use std::{path::PathBuf, process::ExitStatus, time::Duration};

pub type BpResult<T> = Result<T, BpError>;

#[derive(Debug, thiserror::Error)]
pub enum BpError {
    #[error(transparent)]
    Binance(#[from] BinanceError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error(transparent)]
    Data(#[from] DataError),
    #[error(transparent)]
    Exchange(#[from] ExchangeError),
    #[cfg(feature = "github")]
    #[error(transparent)]
    GitHub(#[from] GitHubError),
    #[cfg(feature = "http")]
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error(transparent)]
    Ingester(#[from] IngesterError),
    #[error(transparent)]
    Io(#[from] IoError),
    #[cfg(feature = "lambda")]
    #[error(transparent)]
    Lambda(#[from] LambdaError),
    #[error(transparent)]
    RateLimit(#[from] RateLimitError),
    #[error(transparent)]
    Secrets(#[from] SecretsError),
    #[error(transparent)]
    Sqs(#[from] SqsError),
    #[cfg(feature = "storage")]
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    System(#[from] SystemError),
    #[error(transparent)]
    Timeout(#[from] TimeoutError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
}

impl BpError {
    pub fn config_missing(name: impl Into<String>) -> Self {
        Self::Config(ConfigError::Missing { name: name.into() })
    }

    pub fn config_empty(name: impl Into<String>) -> Self {
        Self::Config(ConfigError::Empty { name: name.into() })
    }

    pub fn config_not_unicode(name: impl Into<String>) -> Self {
        Self::Config(ConfigError::NotUnicode { name: name.into() })
    }

    pub fn config_invalid(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Config(ConfigError::Invalid {
            name: name.into(),
            message: message.into(),
            source: None,
        })
    }

    pub fn config_invalid_source(
        name: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Config(ConfigError::Invalid {
            name: name.into(),
            message: source.to_string(),
            source: Some(Box::new(source)),
        })
    }

    pub fn invalid_path(value: impl Into<String>) -> Self {
        Self::Validation(ValidationError::InvalidPath {
            value: value.into(),
        })
    }

    pub fn io<E>(operation: &'static str, path: impl Into<String>, source: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Io(IoError::Io {
            operation,
            path: path.into(),
            source: source.into(),
        })
    }

    #[cfg(feature = "storage")]
    pub fn s3<E>(operation: &'static str, target: impl Into<String>, source: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::s3_with_retryability(operation, target, true, source)
    }

    #[cfg(feature = "storage")]
    pub fn s3_with_retryability<E>(
        operation: &'static str,
        target: impl Into<String>,
        retryable: bool,
        source: E,
    ) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Storage(StorageError::S3 {
            operation,
            target: target.into(),
            retryable,
            source: source.into(),
        })
    }

    pub fn exchange<E>(operation: &'static str, source: E) -> Self
    where
        E: Into<BpError>,
    {
        Self::Exchange(ExchangeError::Exchange {
            operation,
            source: Box::new(source.into()),
        })
    }

    pub fn candle_conversion<E>(field: &'static str, source: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Data(DataError::CandleConversion {
            field,
            source: source.into(),
        })
    }

    #[cfg(feature = "http")]
    pub fn http_request(
        operation: &'static str,
        target: impl Into<String>,
        source: reqwest::Error,
    ) -> Self {
        Self::Http(HttpError::Request {
            operation,
            target: target.into(),
            source,
        })
    }

    #[cfg(feature = "http")]
    pub fn http_rejected(
        operation: &'static str,
        target: impl Into<String>,
        status: u16,
        body: impl Into<String>,
    ) -> Self {
        Self::Http(HttpError::Rejected {
            operation,
            target: target.into(),
            status,
            body: body.into(),
        })
    }

    pub fn command_failed(
        operation: &'static str,
        path: impl Into<PathBuf>,
        status: ExitStatus,
    ) -> Self {
        Self::Io(IoError::CommandFailed {
            operation,
            path: path.into(),
            status,
        })
    }

    pub fn external(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::System(SystemError::External(source.into()))
    }

    pub fn send_not_sent(source: Self) -> Self {
        Self::Connector(ConnectorError::send_not_sent(source))
    }

    pub fn send_not_sent_external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Connector(ConnectorError::send_not_sent_external(source))
    }

    pub fn send_failed(source: Self) -> Self {
        Self::Connector(ConnectorError::send_failed(source))
    }

    pub fn send_unknown(source: Self) -> Self {
        Self::Connector(ConnectorError::send_unknown(source))
    }

    pub fn send_unknown_external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Connector(ConnectorError::send_unknown_external(source))
    }

    pub fn multiple(failures: impl IntoIterator<Item = BpError>) -> Self {
        Self::Data(DataError::multiple(failures))
    }

    pub fn is_skippable(&self) -> bool {
        matches!(
            self,
            Self::Exchange(
                ExchangeError::MissingSymbol { .. }
                    | ExchangeError::UnsupportedExchange { .. }
                    | ExchangeError::UnsupportedTradingMode { .. }
            ) | Self::Data(DataError::LockContention { .. })
        )
    }

    pub fn any_sent(&self) -> bool {
        matches!(self, Self::Sqs(error) if error.any_sent())
    }

    pub fn is_client_error(&self) -> bool {
        #[cfg(feature = "github")]
        {
            matches!(
                self,
                Self::GitHub(GitHubError::HostInvalid { .. } | GitHubError::HostNotAllowed { .. })
            )
        }
        #[cfg(not(feature = "github"))]
        {
            false
        }
    }

    pub fn send_failure(&self) -> Option<SendFailure> {
        match self {
            Self::Connector(error) => error.send_failure(),
            _ => None,
        }
    }

    pub fn error_type(&self) -> &'static str {
        match self {
            Self::Binance(error) => error.error_type(),
            Self::Config(error) => error.error_type(),
            Self::Connector(error) => error.error_type(),
            Self::Data(error) => error.error_type(),
            Self::Exchange(error) => error.error_type(),
            #[cfg(feature = "github")]
            Self::GitHub(error) => error.error_type(),
            #[cfg(feature = "http")]
            Self::Http(error) => error.error_type(),
            Self::Ingester(error) => error.error_type(),
            Self::Io(error) => error.error_type(),
            #[cfg(feature = "lambda")]
            Self::Lambda(error) => error.error_type(),
            Self::RateLimit(error) => error.error_type(),
            Self::Secrets(error) => error.error_type(),
            Self::Sqs(error) => error.error_type(),
            #[cfg(feature = "storage")]
            Self::Storage(error) => error.error_type(),
            Self::System(error) => error.error_type(),
            Self::Timeout(error) => error.error_type(),
            Self::Validation(error) => error.error_type(),
        }
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Binance(error) => error.retryability(),
            Self::Config(error) => error.retryability(),
            Self::Connector(error) => error.retryability(),
            Self::Data(error) => error.retryability(),
            Self::Exchange(error) => error.retryability(),
            #[cfg(feature = "github")]
            Self::GitHub(error) => error.retryability(),
            #[cfg(feature = "http")]
            Self::Http(error) => error.retryability(),
            Self::Ingester(error) => error.retryability(),
            Self::Io(error) => error.retryability(),
            #[cfg(feature = "lambda")]
            Self::Lambda(error) => error.retryability(),
            Self::RateLimit(error) => error.retryability(),
            Self::Secrets(error) => error.retryability(),
            Self::Sqs(error) => error.retryability(),
            #[cfg(feature = "storage")]
            Self::Storage(error) => error.retryability(),
            Self::System(error) => error.retryability(),
            Self::Timeout(error) => error.retryability(),
            Self::Validation(error) => error.retryability(),
        }
    }
}

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
            } => Self::Exchange(ExchangeError::BadValue {
                name,
                value,
                reason: source.to_string(),
            }),
            ProtocolError::UnsupportedScheme {
                name,
                value,
                allowed,
                scheme,
            } => Self::Exchange(ExchangeError::BadValue {
                name,
                value,
                reason: format!(
                    "URL scheme must be one of {}, got {scheme}",
                    allowed.join(", ")
                ),
            }),
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
