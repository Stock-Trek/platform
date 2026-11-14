use platform_exit_codes::ExitCode;
use std::{fmt, io, path::PathBuf};
use stock_trek::{errors::StockTrekError, types::AssetId, types::CexId};
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub enum Input {
    StrategyContext,
    Portfolio,
    MarketData,
}

impl Input {
    fn not_found(self) -> ExitCode {
        match self {
            Self::StrategyContext => ExitCode::ServerRunnerStrategyContextFileNotFound,
            Self::Portfolio => ExitCode::ServerRunnerPortfolioFileNotFound,
            Self::MarketData => ExitCode::ServerRunnerMarketDataFileNotFound,
        }
    }

    fn not_decoded(self) -> ExitCode {
        match self {
            Self::StrategyContext => ExitCode::ServerRunnerStrategyContextNotDecoded,
            Self::Portfolio => ExitCode::ServerRunnerPortfolioNotDecoded,
            Self::MarketData => ExitCode::ServerRunnerMarketDataNotDecoded,
        }
    }
}

impl fmt::Display for Input {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StrategyContext => "strategy context",
            Self::Portfolio => "portfolio",
            Self::MarketData => "market data",
        })
    }
}

#[derive(Debug, Error)]
pub enum BacktestError {
    #[error("invalid backtest request: {0}")]
    InvalidArgument(String),
    #[error("command is not a back test")]
    UnsupportedCommand,
    #[error("failed to read {input} from {path}: {source}")]
    ReadInput {
        input: Input,
        path: PathBuf,
        source: io::Error,
    },
    #[error("failed to decode {input} from {path}: {source}")]
    DecodeInput {
        input: Input,
        path: PathBuf,
        source: prost::DecodeError,
    },
    #[error("portfolio could not be created: {0}")]
    InvalidPortfolio(StockTrekError),
    #[error("market data is invalid: {0}")]
    InvalidMarketData(String),
    #[error("no market data for an order on {cex} {base}/{quote}")]
    MissingMarket {
        cex: CexId,
        base: AssetId,
        quote: AssetId,
    },
    #[error("unsupported action in back test")]
    UnsupportedAction,
    #[error("failed to create scratch pad: {0}")]
    ScratchPad(io::Error),
    #[error("failed to write guest input {path}: {source}")]
    WriteGuestInput { path: PathBuf, source: io::Error },
    #[error("guest step failed with exit code {0}")]
    GuestStepFailed(i32),
    #[error("action event is missing its action field")]
    MissingAction,
    #[error("action could not be decoded: {0}")]
    InvalidAction(serde_json::Error),
    #[error("failed to start tokio runtime: {0}")]
    RuntimeStart(io::Error),
    #[error("failed to publish simulation result: {0}")]
    Publish(String),
    #[error("simulation panicked")]
    Panic,
}

impl BacktestError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    pub fn invalid_market_data(message: impl Into<String>) -> Self {
        Self::InvalidMarketData(message.into())
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            Self::InvalidArgument(_) => ExitCode::ServerEntryInvalidArgument.code(),
            Self::UnsupportedCommand => ExitCode::ServerEntryFailedToParseRequest.code(),
            Self::ReadInput { input, source, .. } if source.kind() == io::ErrorKind::NotFound => {
                input.not_found().code()
            }
            Self::ReadInput { input, .. } | Self::DecodeInput { input, .. } => {
                input.not_decoded().code()
            }
            Self::InvalidPortfolio(_) => ExitCode::ServerRunnerPortfolioNotCreated.code(),
            Self::InvalidMarketData(_) => ExitCode::ServerRunnerMarketDataNotDecoded.code(),
            Self::MissingMarket { .. } | Self::UnsupportedAction | Self::MissingAction => {
                ExitCode::BotErrorResolvingAction.code()
            }
            Self::InvalidAction(_) => ExitCode::BotErrorResolvingAction.code(),
            Self::ScratchPad(_) | Self::WriteGuestInput { .. } => {
                ExitCode::BotErrorCreatingScratchPad.code()
            }
            Self::GuestStepFailed(code) => *code,
            Self::RuntimeStart(_) => ExitCode::ServerRunnerRuntimeStartFailed.code(),
            Self::Publish(_) => ExitCode::ServerRunnerResultPublishFailed.code(),
            Self::Panic => ExitCode::BotErrorPanic.code(),
        }
    }
}

impl From<BacktestError> for i32 {
    fn from(error: BacktestError) -> Self {
        error.exit_code()
    }
}
