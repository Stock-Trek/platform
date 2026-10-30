use super::BpError;
#[cfg(feature = "http")]
use crate::http::HttpError;
#[cfg(feature = "storage")]
use crate::storage::StorageError;
use crate::{
    config::ConfigError, connector::ConnectorError, data::DataError, exchange::ExchangeError,
    io::IoError, system::SystemError, validation::ValidationError,
};
use std::{path::PathBuf, process::ExitStatus};

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

    pub fn bad_value(
        name: impl Into<String>,
        value: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::Exchange(ExchangeError::BadValue {
            name: name.into(),
            value: value.into(),
            reason: reason.into(),
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
}
