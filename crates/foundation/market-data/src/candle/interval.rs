//! Candle window helpers backed by stock-trek's [`AlignedWindow`].
//!
//! [`AlignedWindow`] is the source of truth for candle intervals. This module
//! only adds the alignment and parsing helpers that platform needs, and it
//! rejects windows it does not explicitly know how to align instead of
//! silently falling back to a fixed duration.

use chrono::{DateTime, Datelike, Months, TimeZone, Utc};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
pub use stock_trek::markets::AlignedWindow;
use strum::IntoEnumIterator;
use thiserror::Error;

/// Errors produced while parsing or aligning a candle window.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlignedWindowError {
    #[error("invalid candle interval label {0:?}")]
    InvalidLabel(String),
    #[error("unsupported candle interval window {0:?}")]
    UnsupportedWindow(AlignedWindow),
    #[error("timestamp {timestamp} is outside the supported range for window {window:?}")]
    TimestampOutOfRange {
        window: AlignedWindow,
        timestamp: i64,
    },
}

/// Parse a candle interval label into its stock-trek aligned window.
///
/// Labels are matched case-insensitively. Only the canonical labels emitted by
/// [`AlignedWindow::label`] are accepted; unknown labels are rejected.
pub fn parse_interval(label: &str) -> Result<AlignedWindow, AlignedWindowError> {
    let original = label.trim();
    let normalized = original.to_ascii_lowercase();
    AlignedWindow::try_from(normalized.as_str())
        .map_err(|_| AlignedWindowError::InvalidLabel(original.to_string()))
}

/// Every candle interval supported by stock-trek.
pub fn all_intervals() -> Vec<AlignedWindow> {
    AlignedWindow::iter().collect()
}

/// Candle-specific operations for stock-trek's aligned windows.
pub trait AlignedWindowExt: Sized {
    /// The fixed duration of this window, or an upper bound for calendar
    /// windows whose exact length depends on the month or year.
    fn duration(self) -> DurationMilliseconds;

    /// The inclusive start and exclusive end of the window containing
    /// `timestamp`.
    fn bounds_for(
        self,
        timestamp: TimestampMilliseconds,
    ) -> Result<(TimestampMilliseconds, TimestampMilliseconds), AlignedWindowError>;

    fn start_for(
        self,
        timestamp: TimestampMilliseconds,
    ) -> Result<TimestampMilliseconds, AlignedWindowError> {
        self.bounds_for(timestamp).map(|(start, _)| start)
    }

    fn end_for(
        self,
        timestamp: TimestampMilliseconds,
    ) -> Result<TimestampMilliseconds, AlignedWindowError> {
        self.bounds_for(timestamp).map(|(_, end)| end)
    }
}

impl AlignedWindowExt for AlignedWindow {
    fn duration(self) -> DurationMilliseconds {
        DurationMilliseconds(i64::try_from(self.millis_upper_bound()).unwrap_or(i64::MAX))
    }

    fn bounds_for(
        self,
        timestamp: TimestampMilliseconds,
    ) -> Result<(TimestampMilliseconds, TimestampMilliseconds), AlignedWindowError> {
        if timestamp.0 < 0 {
            return Err(AlignedWindowError::TimestampOutOfRange {
                window: self,
                timestamp: timestamp.0,
            });
        }
        let months = match self {
            AlignedWindow::Months1 => 1,
            AlignedWindow::Months2 => 2,
            AlignedWindow::Months3 => 3,
            AlignedWindow::Months4 => 4,
            AlignedWindow::Months6 => 6,
            AlignedWindow::Months12 => 12,
            AlignedWindow::Years1 => 12,
            AlignedWindow::Years2 => 24,
            AlignedWindow::Seconds1
            | AlignedWindow::Seconds5
            | AlignedWindow::Seconds10
            | AlignedWindow::Seconds15
            | AlignedWindow::Seconds30
            | AlignedWindow::Seconds60
            | AlignedWindow::Minutes1
            | AlignedWindow::Minutes2
            | AlignedWindow::Minutes3
            | AlignedWindow::Minutes5
            | AlignedWindow::Minutes10
            | AlignedWindow::Minutes15
            | AlignedWindow::Minutes30
            | AlignedWindow::Minutes60
            | AlignedWindow::Hours1
            | AlignedWindow::Hours2
            | AlignedWindow::Hours3
            | AlignedWindow::Hours4
            | AlignedWindow::Hours6
            | AlignedWindow::Hours8
            | AlignedWindow::Hours12
            | AlignedWindow::Hours24
            | AlignedWindow::Days1
            | AlignedWindow::Days7
            | AlignedWindow::Weeks1
            | AlignedWindow::Weeks2
            | AlignedWindow::Weeks4 => {
                return fixed_bounds(self, timestamp);
            }
            _ => return Err(AlignedWindowError::UnsupportedWindow(self)),
        };
        calendar_bounds(self, timestamp, months)
    }
}

impl AlignedWindowExt for &AlignedWindow {
    fn duration(self) -> DurationMilliseconds {
        (*self).duration()
    }

    fn bounds_for(
        self,
        timestamp: TimestampMilliseconds,
    ) -> Result<(TimestampMilliseconds, TimestampMilliseconds), AlignedWindowError> {
        (*self).bounds_for(timestamp)
    }
}

fn fixed_bounds(
    window: AlignedWindow,
    timestamp: TimestampMilliseconds,
) -> Result<(TimestampMilliseconds, TimestampMilliseconds), AlignedWindowError> {
    let duration = i64::try_from(window.millis_upper_bound()).map_err(|_| {
        AlignedWindowError::TimestampOutOfRange {
            window,
            timestamp: timestamp.0,
        }
    })?;
    let start = timestamp.0.div_euclid(duration) * duration;
    let end = start
        .checked_add(duration)
        .ok_or(AlignedWindowError::TimestampOutOfRange {
            window,
            timestamp: timestamp.0,
        })?;
    Ok((TimestampMilliseconds(start), TimestampMilliseconds(end)))
}

fn calendar_bounds(
    window: AlignedWindow,
    timestamp: TimestampMilliseconds,
    months: u32,
) -> Result<(TimestampMilliseconds, TimestampMilliseconds), AlignedWindowError> {
    let out_of_range = || AlignedWindowError::TimestampOutOfRange {
        window,
        timestamp: timestamp.0,
    };
    let datetime = DateTime::<Utc>::from_timestamp_millis(timestamp.0).ok_or_else(out_of_range)?;
    let month_index = i64::from(datetime.year()) * 12 + i64::from(datetime.month0());
    let months_i64 = i64::from(months);
    let start_month = month_index.div_euclid(months_i64) * months_i64;
    let start_year = i32::try_from(start_month.div_euclid(12)).map_err(|_| out_of_range())?;
    let start_month_of_year =
        u32::try_from(start_month.rem_euclid(12) + 1).map_err(|_| out_of_range())?;
    let start = Utc
        .with_ymd_and_hms(start_year, start_month_of_year, 1, 0, 0, 0)
        .single()
        .ok_or_else(out_of_range)?;
    let end = start
        .checked_add_months(Months::new(months))
        .ok_or_else(out_of_range)?;
    Ok((
        TimestampMilliseconds(start.timestamp_millis()),
        TimestampMilliseconds(end.timestamp_millis()),
    ))
}

/// Serialize an aligned window using its canonical label.
///
/// This keeps candle keys and revisions compatible with the label-based format
/// used by archive records while the in-memory type is stock-trek's
/// [`AlignedWindow`].
pub(crate) mod serde_label {
    use super::{AlignedWindow, parse_interval};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S>(window: &AlignedWindow, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&window.label())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<AlignedWindow, D::Error>
    where
        D: Deserializer<'de>,
    {
        let label = String::deserialize(deserializer)?;
        parse_interval(&label).map_err(serde::de::Error::custom)
    }
}
