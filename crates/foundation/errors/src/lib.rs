mod auth;
#[cfg(test)]
mod auth_tests;
mod binance;
mod build;
mod build_classification;
mod build_constructors;
mod build_conversions;
mod config;
mod connector;
#[cfg(test)]
mod connector_tests;
mod data;
#[cfg(test)]
mod data_tests;
mod decimal;
mod error_type;
mod exchange;
#[cfg(feature = "github")]
mod github;
#[cfg(feature = "http")]
mod http;
mod ingester;
mod io;
#[cfg(test)]
mod io_tests;
#[cfg(feature = "lambda")]
mod lambda;
mod nats;
mod rate_limit;
mod retryability;
#[cfg(test)]
mod retryability_tests;
mod secrets;
#[cfg(test)]
mod secrets_tests;
mod sqs;
#[cfg(feature = "storage")]
mod storage;
#[cfg(test)]
mod storage_tests;
mod system;
#[cfg(test)]
mod system_tests;
mod timeout;
mod validation;

pub use auth::AuthError;
pub use binance::{BinanceError, BinanceErrorData};
pub use build::{BuildError, BuildResult};
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
pub use rate_limit::RateLimitError;
pub use retryability::Retryability;
pub use secrets::SecretsError;
pub use sqs::{SqsBatchFailure, SqsError};
#[cfg(feature = "storage")]
pub use storage::{S3DeleteObjectFailure, StorageError};
pub use system::SystemError;
pub use timeout::TimeoutError;
pub use validation::ValidationError;
