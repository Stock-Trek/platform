use chrono::{DateTime, Datelike, Months, TimeZone, Utc};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;
use stock_trek::markets::AlignedWindow;
use strum::IntoEnumIterator;
use thiserror::Error;

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: i64 = 24 * MILLIS_PER_HOUR;
const MILLIS_PER_MONTH_UPPER_BOUND: i64 = 31 * MILLIS_PER_DAY;
const MILLIS_PER_YEAR_UPPER_BOUND: i64 = 366 * MILLIS_PER_DAY;

/// A candle interval backed by the stock-trek [`AlignedWindow`] set.
///
/// Aligned windows with the same fixed duration (for example `Seconds60` and
/// `Minutes1`) are canonicalized to one interval so a trade updates exactly
/// one candle per duration. Calendar windows keep their month or year identity
/// because their length depends on the calendar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CandleInterval {
    window: AlignedWindow,
}

impl CandleInterval {
    /// Every distinct interval exposed by stock-trek's [`AlignedWindow`].
    pub fn all() -> Vec<Self> {
        let mut intervals = AlignedWindow::iter()
            .map(Self::from_aligned_window)
            .collect::<Vec<_>>();
        intervals.sort();
        intervals.dedup();
        intervals
    }

    pub fn from_aligned_window(window: AlignedWindow) -> Self {
        Self {
            window: Self::canonical_window(window),
        }
    }

    pub fn from_label(label: &str) -> Result<Self, CandleIntervalError> {
        let label = label.trim().to_ascii_lowercase();
        let window = Self::window_from_label(&label)
            .ok_or_else(|| CandleIntervalError::InvalidLabel(label.clone()))?;
        Ok(Self::from_aligned_window(window))
    }

    pub fn label(self) -> String {
        match self.window {
            AlignedWindow::Seconds5 => "5s".to_string(),
            AlignedWindow::Seconds10 => "10s".to_string(),
            AlignedWindow::Seconds15 => "15s".to_string(),
            AlignedWindow::Seconds30 => "30s".to_string(),
            AlignedWindow::Seconds60 | AlignedWindow::Minutes1 => "1m".to_string(),
            AlignedWindow::Minutes2 => "2m".to_string(),
            AlignedWindow::Minutes3 => "3m".to_string(),
            AlignedWindow::Minutes5 => "5m".to_string(),
            AlignedWindow::Minutes10 => "10m".to_string(),
            AlignedWindow::Minutes15 => "15m".to_string(),
            AlignedWindow::Minutes30 => "30m".to_string(),
            AlignedWindow::Minutes60 | AlignedWindow::Hours1 => "1h".to_string(),
            AlignedWindow::Hours2 => "2h".to_string(),
            AlignedWindow::Hours3 => "3h".to_string(),
            AlignedWindow::Hours4 => "4h".to_string(),
            AlignedWindow::Hours6 => "6h".to_string(),
            AlignedWindow::Hours8 => "8h".to_string(),
            AlignedWindow::Hours12 => "12h".to_string(),
            AlignedWindow::Hours24 | AlignedWindow::Days1 => "1d".to_string(),
            AlignedWindow::Days7 | AlignedWindow::Weeks1 => "7d".to_string(),
            AlignedWindow::Weeks2 => "14d".to_string(),
            AlignedWindow::Weeks4 => "28d".to_string(),
            AlignedWindow::Months1 => "1mo".to_string(),
            AlignedWindow::Months2 => "2mo".to_string(),
            AlignedWindow::Months3 => "3mo".to_string(),
            AlignedWindow::Months4 => "4mo".to_string(),
            AlignedWindow::Months6 => "6mo".to_string(),
            AlignedWindow::Months12 => "12mo".to_string(),
            AlignedWindow::Years1 => "1y".to_string(),
            AlignedWindow::Years2 => "2y".to_string(),
            _ => "unknown".to_string(),
        }
    }

    /// The fixed duration of this interval.
    ///
    /// Calendar intervals do not have one exact duration. This returns their
    /// upper bound so callers that need a conservative allocation can still
    /// use it; candle windows themselves use the actual calendar bounds.
    pub fn duration(self) -> DurationMilliseconds {
        DurationMilliseconds(match self.window {
            AlignedWindow::Months1 => MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months2 => 2 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months3 => 3 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months4 => 4 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months6 => 6 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months12 => 12 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Years1 => MILLIS_PER_YEAR_UPPER_BOUND,
            AlignedWindow::Years2 => 2 * MILLIS_PER_YEAR_UPPER_BOUND,
            window => Self::fixed_duration(window),
        })
    }

    pub fn start_for(self, timestamp: TimestampMilliseconds) -> TimestampMilliseconds {
        TimestampMilliseconds(self.bounds_for(timestamp).0)
    }

    pub fn end_for(self, timestamp: TimestampMilliseconds) -> TimestampMilliseconds {
        TimestampMilliseconds(self.bounds_for(timestamp).1)
    }

    fn bounds_for(self, timestamp: TimestampMilliseconds) -> (i64, i64) {
        match self.window {
            AlignedWindow::Months1 => calendar_bounds(timestamp.0, 1),
            AlignedWindow::Months2 => calendar_bounds(timestamp.0, 2),
            AlignedWindow::Months3 => calendar_bounds(timestamp.0, 3),
            AlignedWindow::Months4 => calendar_bounds(timestamp.0, 4),
            AlignedWindow::Months6 => calendar_bounds(timestamp.0, 6),
            AlignedWindow::Months12 => calendar_bounds(timestamp.0, 12),
            AlignedWindow::Years1 => calendar_bounds(timestamp.0, 12),
            AlignedWindow::Years2 => calendar_bounds(timestamp.0, 24),
            window => {
                let duration = Self::fixed_duration(window);
                let start = timestamp.0.div_euclid(duration) * duration;
                (start, start + duration)
            }
        }
    }

    fn canonical_window(window: AlignedWindow) -> AlignedWindow {
        match window {
            AlignedWindow::Seconds60 => AlignedWindow::Minutes1,
            AlignedWindow::Minutes60 => AlignedWindow::Hours1,
            AlignedWindow::Hours24 => AlignedWindow::Days1,
            AlignedWindow::Weeks1 => AlignedWindow::Days7,
            window => window,
        }
    }

    fn fixed_duration(window: AlignedWindow) -> i64 {
        match window {
            AlignedWindow::Seconds5 => 5 * MILLIS_PER_SECOND,
            AlignedWindow::Seconds10 => 10 * MILLIS_PER_SECOND,
            AlignedWindow::Seconds15 => 15 * MILLIS_PER_SECOND,
            AlignedWindow::Seconds30 => 30 * MILLIS_PER_SECOND,
            AlignedWindow::Seconds60 | AlignedWindow::Minutes1 => MILLIS_PER_MINUTE,
            AlignedWindow::Minutes2 => 2 * MILLIS_PER_MINUTE,
            AlignedWindow::Minutes3 => 3 * MILLIS_PER_MINUTE,
            AlignedWindow::Minutes5 => 5 * MILLIS_PER_MINUTE,
            AlignedWindow::Minutes10 => 10 * MILLIS_PER_MINUTE,
            AlignedWindow::Minutes15 => 15 * MILLIS_PER_MINUTE,
            AlignedWindow::Minutes30 => 30 * MILLIS_PER_MINUTE,
            AlignedWindow::Minutes60 | AlignedWindow::Hours1 => MILLIS_PER_HOUR,
            AlignedWindow::Hours2 => 2 * MILLIS_PER_HOUR,
            AlignedWindow::Hours3 => 3 * MILLIS_PER_HOUR,
            AlignedWindow::Hours4 => 4 * MILLIS_PER_HOUR,
            AlignedWindow::Hours6 => 6 * MILLIS_PER_HOUR,
            AlignedWindow::Hours8 => 8 * MILLIS_PER_HOUR,
            AlignedWindow::Hours12 => 12 * MILLIS_PER_HOUR,
            AlignedWindow::Hours24 | AlignedWindow::Days1 => MILLIS_PER_DAY,
            AlignedWindow::Days7 | AlignedWindow::Weeks1 => 7 * MILLIS_PER_DAY,
            AlignedWindow::Weeks2 => 14 * MILLIS_PER_DAY,
            AlignedWindow::Weeks4 => 28 * MILLIS_PER_DAY,
            AlignedWindow::Months1 => MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months2 => 2 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months3 => 3 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months4 => 4 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months6 => 6 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Months12 => 12 * MILLIS_PER_MONTH_UPPER_BOUND,
            AlignedWindow::Years1 => MILLIS_PER_YEAR_UPPER_BOUND,
            AlignedWindow::Years2 => 2 * MILLIS_PER_YEAR_UPPER_BOUND,
            _ => MILLIS_PER_DAY,
        }
    }

    fn window_from_label(label: &str) -> Option<AlignedWindow> {
        let window = match label {
            "5s" | "seconds5" => AlignedWindow::Seconds5,
            "10s" | "seconds10" => AlignedWindow::Seconds10,
            "15s" | "seconds15" => AlignedWindow::Seconds15,
            "30s" | "seconds30" => AlignedWindow::Seconds30,
            "1m" | "60s" | "minutes1" => AlignedWindow::Minutes1,
            "2m" | "minutes2" => AlignedWindow::Minutes2,
            "3m" | "minutes3" => AlignedWindow::Minutes3,
            "5m" | "minutes5" => AlignedWindow::Minutes5,
            "10m" | "minutes10" => AlignedWindow::Minutes10,
            "15m" | "minutes15" => AlignedWindow::Minutes15,
            "30m" | "minutes30" => AlignedWindow::Minutes30,
            "1h" | "60m" | "hours1" => AlignedWindow::Hours1,
            "2h" | "hours2" => AlignedWindow::Hours2,
            "3h" | "hours3" => AlignedWindow::Hours3,
            "4h" | "hours4" => AlignedWindow::Hours4,
            "6h" | "hours6" => AlignedWindow::Hours6,
            "8h" | "hours8" => AlignedWindow::Hours8,
            "12h" | "hours12" => AlignedWindow::Hours12,
            "1d" | "24h" | "hours24" | "days1" => AlignedWindow::Days1,
            "7d" | "1w" | "weeks1" | "days7" => AlignedWindow::Days7,
            "14d" | "2w" | "weeks2" => AlignedWindow::Weeks2,
            "28d" | "4w" | "weeks4" => AlignedWindow::Weeks4,
            "1mo" | "months1" => AlignedWindow::Months1,
            "2mo" | "months2" => AlignedWindow::Months2,
            "3mo" | "months3" => AlignedWindow::Months3,
            "4mo" | "months4" => AlignedWindow::Months4,
            "6mo" | "months6" => AlignedWindow::Months6,
            "12mo" | "months12" => AlignedWindow::Months12,
            "1y" | "years1" => AlignedWindow::Years1,
            "2y" | "years2" => AlignedWindow::Years2,
            _ => return Self::legacy_duration_from_label(label),
        };
        Some(window)
    }

    fn legacy_duration_from_label(label: &str) -> Option<AlignedWindow> {
        let split_at = label
            .find(|character: char| !character.is_ascii_digit())
            .or_else(|| (!label.is_empty()).then_some(label.len()))?;
        let (amount, unit) = label.split_at(split_at);
        let amount: i64 = amount.parse().ok()?;
        let duration = amount.checked_mul(match unit {
            "ms" => 1,
            "s" => MILLIS_PER_SECOND,
            "m" => MILLIS_PER_MINUTE,
            "h" => MILLIS_PER_HOUR,
            "d" => MILLIS_PER_DAY,
            _ => return None,
        })?;
        if duration <= 0 {
            return None;
        }
        AlignedWindow::iter()
            .find(|window| !Self::is_calendar(*window) && Self::fixed_duration(*window) == duration)
    }

    fn from_serialized_duration(duration: i64) -> Option<Self> {
        AlignedWindow::iter()
            .find(|window| !Self::is_calendar(*window) && Self::fixed_duration(*window) == duration)
            .map(Self::from_aligned_window)
    }

    fn is_calendar(window: AlignedWindow) -> bool {
        matches!(
            window,
            AlignedWindow::Months1
                | AlignedWindow::Months2
                | AlignedWindow::Months3
                | AlignedWindow::Months4
                | AlignedWindow::Months6
                | AlignedWindow::Months12
                | AlignedWindow::Years1
                | AlignedWindow::Years2
        )
    }
}

impl PartialOrd for CandleInterval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CandleInterval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.window as u8).cmp(&(other.window as u8))
    }
}

fn calendar_bounds(timestamp_millis: i64, months: u32) -> (i64, i64) {
    let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_millis)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let month_index = i64::from(timestamp.year()) * 12 + i64::from(timestamp.month0());
    let months = i64::from(months);
    let start_month = month_index.div_euclid(months) * months;
    let start_year = i32::try_from(start_month.div_euclid(12)).unwrap_or(1970);
    let start_month_of_year = u32::try_from(start_month.rem_euclid(12) + 1).unwrap_or(1);
    let start = Utc
        .with_ymd_and_hms(start_year, start_month_of_year, 1, 0, 0, 0)
        .single()
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let end = start
        .checked_add_months(Months::new(u32::try_from(months).unwrap_or(1)))
        .unwrap_or(start);
    (start.timestamp_millis(), end.timestamp_millis())
}

impl Serialize for CandleInterval {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if Self::is_calendar(self.window) {
            serializer.serialize_str(&self.label())
        } else {
            serializer.serialize_i64(self.duration().0)
        }
    }
}

impl<'de> Deserialize<'de> for CandleInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IntervalVisitor;

        impl<'de> de::Visitor<'de> for IntervalVisitor {
            type Value = CandleInterval;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a candle interval label or millisecond duration")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                CandleInterval::from_serialized_duration(value).ok_or_else(|| {
                    E::custom(format!("unsupported candle interval duration {value}"))
                })
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = i64::try_from(value).map_err(|_| {
                    E::custom(format!("unsupported candle interval duration {value}"))
                })?;
                self.visit_i64(value)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                CandleInterval::from_label(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(IntervalVisitor)
    }
}

impl fmt::Display for CandleInterval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.label())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CandleIntervalError {
    #[error("invalid candle interval {0:?}; expected an AlignedWindow value such as 5m or 1h")]
    InvalidLabel(String),
    #[error("candle interval must be greater than zero and representable in milliseconds")]
    ZeroOrOverflow,
}
