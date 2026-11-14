mod auth;
mod binance;
mod config;
mod connector;
mod data;
mod decimal;
mod error_type;
mod exchange;
#[cfg(feature = "github")]
mod github;
#[cfg(feature = "http")]
mod http;
mod ingester;
mod io;
#[cfg(feature = "lambda")]
mod lambda;
mod nats;
mod platform;
mod platform_classification;
mod platform_constructors;
mod platform_conversions;
mod rate_limit;
mod retryability;
mod secrets;
mod sqs;
#[cfg(feature = "storage")]
mod storage;
mod system;
mod timeout;
mod validation;

#[cfg(test)]
mod auth_tests;
#[cfg(test)]
mod connector_tests;
#[cfg(test)]
mod data_tests;
#[cfg(test)]
mod io_tests;
#[cfg(test)]
mod retryability_tests;
#[cfg(test)]
mod secrets_tests;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod system_tests;

pub use auth::AuthError;
pub use binance::{BinanceError, BinanceErrorData};
pub use config::ConfigError;
pub use connector::{ConnectorError, SendFailure};
pub use data::DataError;
pub use decimal::DecimalScaleError;
pub use error_type::ErrorType;
pub use exchange::ExchangeError;
#[cfg(feature = "github")]
pub use github::GitHubError;
#[cfg(feature = "http")]
pub use http::HttpError;
pub use ingester::IngesterError;
pub use io::IoError;
#[cfg(feature = "lambda")]
pub use lambda::LambdaError;
pub use nats::NatsError;
pub use platform::{PlatformError, PlatformResult};
pub use rate_limit::RateLimitError;
pub use retryability::Retryability;
pub use secrets::SecretsError;
pub use sqs::{SqsBatchFailure, SqsError};
#[cfg(feature = "storage")]
pub use storage::{S3DeleteObjectFailure, StorageError};
pub use system::SystemError;
pub use timeout::TimeoutError;
pub use validation::ValidationError;
