use platform_errors::{BpError, BpResult, DataError};
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc, Weekday};

pub struct QuantizedTime;

#[derive(Debug, Clone, Copy)]
pub enum Quantization {
    Minutes(TopOfMinute),
    Hour(TopOfHour),
    Day,
    Week(Weekday),
    Month(TopOfMonth),
    Year,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfMinute {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Five = 5,
    Six = 6,
    Ten = 10,
    Twelve = 12,
    Fifteen = 15,
    Twenty = 20,
    Thirty = 30,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfHour {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Six = 6,
    Twelve = 12,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfMonth {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Six = 6,
}

impl Quantization {
    /// Maximum number of one-minute candles a single window of this quantization can contain.
    ///
    /// Month and year windows vary in length, so their values are upper bounds.
    pub const fn max_window_candles(self) -> u32 {
        const MINUTES_PER_DAY: u32 = 24 * 60;

        match self {
            Quantization::Minutes(top_of_minute) => top_of_minute as u32,
            Quantization::Hour(top_of_hour) => top_of_hour as u32 * 60,
            Quantization::Day => MINUTES_PER_DAY,
            Quantization::Week(_) => 7 * MINUTES_PER_DAY,
            Quantization::Month(top_of_month) => top_of_month as u32 * 31 * MINUTES_PER_DAY,
            Quantization::Year => 366 * MINUTES_PER_DAY,
        }
    }
}

impl QuantizedTime {
    pub fn timestamp(
        timestamp: DateTime<Utc>,
        quantization: Quantization,
    ) -> BpResult<DateTime<Utc>> {
        match quantization {
            Quantization::Minutes(top_of_minute) => Self::minute(timestamp, top_of_minute),
            Quantization::Hour(top_of_hour) => Self::hour(timestamp, top_of_hour),
            Quantization::Day => Self::day(timestamp),
            Quantization::Week(weekday) => Self::week(timestamp, weekday),
            Quantization::Month(top_of_month) => Self::month(timestamp, top_of_month),
            Quantization::Year => Self::year(timestamp),
        }
    }

    /// First quantization boundary at or after `timestamp`.
    pub fn top_at_or_after(
        timestamp: DateTime<Utc>,
        quantization: Quantization,
    ) -> BpResult<DateTime<Utc>> {
        let top = Self::timestamp(timestamp, quantization)?;
        if top < timestamp {
            Self::next_top(top, quantization)
        } else {
            Ok(top)
        }
    }

    pub fn next_top(top: DateTime<Utc>, quantization: Quantization) -> BpResult<DateTime<Utc>> {
        match quantization {
            Quantization::Minutes(top_of_minute) => {
                Self::checked_add(top, Duration::minutes(top_of_minute as i64))
            }
            Quantization::Hour(top_of_hour) => {
                Self::checked_add(top, Duration::hours(top_of_hour as i64))
            }
            Quantization::Day => Self::checked_add(top, Duration::days(1)),
            Quantization::Week(_) => Self::checked_add(top, Duration::weeks(1)),
            Quantization::Month(top_of_month) => {
                let total_months =
                    top.year() as i64 * 12 + top.month0() as i64 + top_of_month as i64;
                let next_year = total_months.div_euclid(12) as i32;
                let next_month = total_months.rem_euclid(12) as u32 + 1;
                Self::with_year_and_month(top, next_year, next_month)
            }
            Quantization::Year => Self::with_year_and_month(top, top.year() + 1, top.month()),
        }
    }

    pub fn previous_top(top: DateTime<Utc>, quantization: Quantization) -> BpResult<DateTime<Utc>> {
        match quantization {
            Quantization::Minutes(top_of_minute) => {
                Self::checked_sub(top, Duration::minutes(top_of_minute as i64))
            }
            Quantization::Hour(top_of_hour) => {
                Self::checked_sub(top, Duration::hours(top_of_hour as i64))
            }
            Quantization::Day => Self::checked_sub(top, Duration::days(1)),
            Quantization::Week(_) => Self::checked_sub(top, Duration::weeks(1)),
            Quantization::Month(top_of_month) => {
                let total_months =
                    top.year() as i64 * 12 + top.month0() as i64 - top_of_month as i64;
                let previous_year = total_months.div_euclid(12) as i32;
                let previous_month = total_months.rem_euclid(12) as u32 + 1;
                Self::with_year_and_month(top, previous_year, previous_month)
            }
            Quantization::Year => Self::with_year_and_month(top, top.year() - 1, top.month()),
        }
    }

    fn minute(timestamp: DateTime<Utc>, top_of_minute: TopOfMinute) -> BpResult<DateTime<Utc>> {
        Self::modulo_seconds(timestamp, (top_of_minute as i64) * 60)
    }
    fn hour(timestamp: DateTime<Utc>, top_of_hour: TopOfHour) -> BpResult<DateTime<Utc>> {
        Self::modulo_seconds(timestamp, (top_of_hour as i64) * 60 * 60)
    }
    fn day(timestamp: DateTime<Utc>) -> BpResult<DateTime<Utc>> {
        Self::modulo_seconds(timestamp, 24 * 60 * 60)
    }
    fn week(timestamp: DateTime<Utc>, weekday: Weekday) -> BpResult<DateTime<Utc>> {
        let current_weekday = timestamp.weekday();
        let days_to_subtract = (current_weekday as i32 - weekday as i32 + 7) % 7;
        let time_delta_to_subtract = Duration::days(days_to_subtract as i64);
        let day_start = timestamp
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange {
                operation: "find start of week",
            }))?
            .and_utc();
        Self::checked_sub(day_start, time_delta_to_subtract)
    }
    fn month(timestamp: DateTime<Utc>, top_of_month: TopOfMonth) -> BpResult<DateTime<Utc>> {
        let year = timestamp.year();
        let month = timestamp.month0() / top_of_month as u32 * top_of_month as u32 + 1;
        Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
            .single()
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange {
                operation: "find start of month",
            }))
    }
    fn year(timestamp: DateTime<Utc>) -> BpResult<DateTime<Utc>> {
        let year = timestamp.year();
        Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
            .single()
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange {
                operation: "find start of year",
            }))
    }
    fn modulo_seconds(timestamp: DateTime<Utc>, modulo: i64) -> BpResult<DateTime<Utc>> {
        let timestamp_secs = timestamp.timestamp();
        let quantized_secs = timestamp_secs - timestamp_secs.rem_euclid(modulo);
        Utc.timestamp_opt(quantized_secs, 0)
            .single()
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange {
                operation: "quantize seconds",
            }))
    }
    fn with_year_and_month(
        timestamp: DateTime<Utc>,
        year: i32,
        month: u32,
    ) -> BpResult<DateTime<Utc>> {
        timestamp
            .with_year(year)
            .and_then(|date_time| date_time.with_month(month))
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange {
                operation: "shift year and month",
            }))
    }
    fn checked_add(timestamp: DateTime<Utc>, duration: Duration) -> BpResult<DateTime<Utc>> {
        timestamp
            .checked_add_signed(duration)
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange {
                operation: "add duration",
            }))
    }
    fn checked_sub(timestamp: DateTime<Utc>, duration: Duration) -> BpResult<DateTime<Utc>> {
        timestamp
            .checked_sub_signed(duration)
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange {
                operation: "subtract duration",
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc, Weekday};

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn top_at_or_after_moves_to_the_next_quantized_top() {
        let six_minutes = Quantization::Minutes(TopOfMinute::Six);
        assert_eq!(
            QuantizedTime::top_at_or_after(utc(2024, 1, 1, 0, 1), six_minutes).unwrap(),
            utc(2024, 1, 1, 0, 6)
        );
        assert_eq!(
            QuantizedTime::top_at_or_after(utc(2024, 1, 1, 0, 6), six_minutes).unwrap(),
            utc(2024, 1, 1, 0, 6)
        );

        let six_hours = Quantization::Hour(TopOfHour::Six);
        assert_eq!(
            QuantizedTime::top_at_or_after(utc(2024, 1, 1, 1, 0), six_hours).unwrap(),
            utc(2024, 1, 1, 6, 0)
        );

        let monday = Quantization::Week(Weekday::Mon);
        assert_eq!(
            QuantizedTime::top_at_or_after(utc(2024, 1, 3, 12, 0), monday).unwrap(),
            utc(2024, 1, 8, 0, 0)
        );

        let six_months = Quantization::Month(TopOfMonth::Six);
        assert_eq!(
            QuantizedTime::top_at_or_after(utc(2024, 2, 15, 0, 0), six_months).unwrap(),
            utc(2024, 7, 1, 0, 0)
        );
    }
}
