pub mod binance;
pub mod bp;
pub mod config;
pub mod connector;
pub mod data;
pub mod decimal;
mod error_type;
pub mod exchange;
#[cfg(feature = "github")]
pub mod github;
#[cfg(feature = "http")]
pub mod http;
pub mod ingester;
pub mod io;
#[cfg(feature = "lambda")]
pub mod lambda;
pub mod rate_limit;
pub mod retryability;
pub mod secrets;
pub mod sqs;
#[cfg(feature = "storage")]
pub mod storage;
pub mod system;
pub mod timeout;
pub mod validation;

pub use binance::{BinanceError, BinanceErrorData};
pub use bp::{BpError, BpResult};
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
pub use rate_limit::RateLimitError;
pub use retryability::Retryability;
pub use secrets::SecretsError;
pub use sqs::{SqsBatchFailure, SqsError};
#[cfg(feature = "storage")]
pub use storage::{S3DeleteObjectFailure, StorageError};
pub use system::SystemError;
pub use timeout::TimeoutError;
pub use validation::ValidationError;
