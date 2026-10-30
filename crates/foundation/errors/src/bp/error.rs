#[cfg(feature = "github")]
use crate::github::GitHubError;
#[cfg(feature = "http")]
use crate::http::HttpError;
#[cfg(feature = "lambda")]
use crate::lambda::LambdaError;
#[cfg(feature = "storage")]
use crate::storage::StorageError;
use crate::{
    binance::BinanceError, config::ConfigError, connector::ConnectorError, data::DataError,
    exchange::ExchangeError, ingester::IngesterError, io::IoError, nats::NatsError,
    rate_limit::RateLimitError, secrets::SecretsError, sqs::SqsError, system::SystemError,
    timeout::TimeoutError, validation::ValidationError,
};

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
    Nats(#[from] NatsError),
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
