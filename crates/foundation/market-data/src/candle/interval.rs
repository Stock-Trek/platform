use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: i64 = 24 * MILLIS_PER_HOUR;

/// A candle interval such as `1s`, `5m`, or `1h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CandleInterval {
    duration: DurationMilliseconds,
}

impl CandleInterval {
    pub fn from_label(label: &str) -> Result<Self, CandleIntervalError> {
        let label = label.trim().to_ascii_lowercase();
        let split_at = label
            .find(|character: char| !character.is_ascii_digit())
            .ok_or_else(|| CandleIntervalError::InvalidLabel(label.clone()))?;
        let (amount, unit) = label.split_at(split_at);
        let amount: i64 = amount
            .parse()
            .map_err(|_| CandleIntervalError::InvalidLabel(label.clone()))?;
        let multiplier = match unit {
            "ms" => 1,
            "s" => MILLIS_PER_SECOND,
            "m" => MILLIS_PER_MINUTE,
            "h" => MILLIS_PER_HOUR,
            "d" => MILLIS_PER_DAY,
            _ => return Err(CandleIntervalError::InvalidLabel(label)),
        };
        let duration = amount
            .checked_mul(multiplier)
            .filter(|duration| *duration > 0)
            .ok_or(CandleIntervalError::ZeroOrOverflow)?;
        Ok(Self {
            duration: DurationMilliseconds(duration),
        })
    }

    pub fn label(self) -> String {
        let duration = self.duration.0;
        if duration % MILLIS_PER_DAY == 0 {
            format!("{}d", duration / MILLIS_PER_DAY)
        } else if duration % MILLIS_PER_HOUR == 0 {
            format!("{}h", duration / MILLIS_PER_HOUR)
        } else if duration % MILLIS_PER_MINUTE == 0 {
            format!("{}m", duration / MILLIS_PER_MINUTE)
        } else if duration % MILLIS_PER_SECOND == 0 {
            format!("{}s", duration / MILLIS_PER_SECOND)
        } else {
            format!("{duration}ms")
        }
    }

    pub fn duration(self) -> DurationMilliseconds {
        self.duration
    }

    pub fn start_for(self, timestamp: TimestampMilliseconds) -> TimestampMilliseconds {
        TimestampMilliseconds(timestamp.0.div_euclid(self.duration.0) * self.duration.0)
    }

    pub fn end_for(self, timestamp: TimestampMilliseconds) -> TimestampMilliseconds {
        self.start_for(timestamp) + self.duration
    }
}

impl fmt::Display for CandleInterval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.label())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CandleIntervalError {
    #[error("invalid candle interval {0:?}; expected a value such as 1s, 5m, or 1h")]
    InvalidLabel(String),
    #[error("candle interval must be greater than zero and representable in milliseconds")]
    ZeroOrOverflow,
}
