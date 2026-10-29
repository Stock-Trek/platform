use crate::{bp::BpError, retryability::Retryability};
use chrono::{DateTime, Utc};
use std::error::Error as StdError;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum DataError {
    #[error("Failed to convert candle {field} to decimal: {source}")]
    CandleConversion {
        field: &'static str,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Date time out of range while trying to {operation}")]
    DateTimeOutOfRange { operation: &'static str },
    #[error("Backfill end time {end} must be after start time {start}")]
    InvalidBackfillRange {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    #[error("Day {day} is not valid for this month")]
    InvalidDay { day: u32 },
    #[error("Hour {hour} must be between 0 and 23")]
    InvalidHour { hour: u32 },
    #[error("Invalid data job: {message}")]
    InvalidJob { message: String },
    #[error("Month {month} must be between 1 and 12")]
    InvalidMonth { month: u32 },
    #[error("Invalid trading pair path {pair}; expected mode/cex/base/quote")]
    InvalidTradingPairPath { pair: String },
    #[error("Failed to parse year and month from path {path}")]
    InvalidYearMonth { path: String },
    #[error("Failed to acquire lock {key}: another invocation is already running")]
    LockContention { key: String },
    #[error("Lost lock {key}: another invocation took over")]
    LockLost { key: String },
    #[error("Lock {key} is missing an ETag and cannot be released safely")]
    MissingLockETag { key: String },
    #[error(
        "{} operation(s) failed: {}",
        .failures.len(),
        Self::format_failures(.failures)
    )]
    Multiple { failures: Vec<BpError> },
    #[error("Cannot get subfolders from path with too many delimiters {path}")]
    TooManyPathSegments { path: String },
    #[error("Trading pair {pair} is not configured for data collection")]
    UnsupportedTradingPair { pair: String },
}

impl DataError {
    pub fn multiple(failures: impl IntoIterator<Item = BpError>) -> Self {
        let failures = failures
            .into_iter()
            .flat_map(Self::flatten_failure)
            .collect::<Vec<_>>();
        Self::Multiple { failures }
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::LockContention { .. } | Self::LockLost { .. } => Retryability::Retryable,
            Self::Multiple { failures }
                if failures
                    .iter()
                    .any(|error| error.retryability().is_retryable()) =>
            {
                Retryability::Retryable
            }
            _ => Retryability::Permanent,
        }
    }

    fn flatten_failure(error: BpError) -> Vec<BpError> {
        match error {
            BpError::Data(Self::Multiple { failures }) => failures,
            error => vec![error],
        }
    }

    fn format_failures(failures: &[BpError]) -> String {
        failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConfigError, TimeoutError};
    use std::time::Duration;

    #[test]
    fn multiple_is_retryable_when_any_failure_is_retryable() {
        let error = BpError::multiple([
            BpError::Config(ConfigError::Missing {
                name: "test".to_string(),
            }),
            BpError::Timeout(TimeoutError::TimedOut),
        ]);

        assert!(error.retryability().is_retryable());
    }

    #[test]
    fn multiple_is_permanent_when_every_failure_is_permanent() {
        let error = BpError::multiple([
            BpError::Config(ConfigError::Missing {
                name: "first".to_string(),
            }),
            BpError::Config(ConfigError::Missing {
                name: "second".to_string(),
            }),
        ]);

        assert!(!error.retryability().is_retryable());
    }

    #[test]
    fn multiple_flattens_nested_failures() {
        let error = BpError::multiple([
            BpError::multiple([BpError::Timeout(TimeoutError::TimedOut)]),
            BpError::Config(ConfigError::Missing {
                name: "test".to_string(),
            }),
        ]);

        let BpError::Data(DataError::Multiple { failures }) = &error else {
            panic!("expected aggregated error");
        };
        assert_eq!(failures.len(), 2);
        assert!(error.retryability().is_retryable());
    }

    #[test]
    fn multiple_display_includes_each_failure() {
        let error = BpError::multiple([
            BpError::Config(ConfigError::Missing {
                name: "first".to_string(),
            }),
            BpError::Timeout(TimeoutError::BuildTimeout(Duration::from_secs(1))),
        ]);

        let message = error.to_string();
        assert!(message.contains("first"));
        assert!(message.contains("build timed out"));
    }
}
