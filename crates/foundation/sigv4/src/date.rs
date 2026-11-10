use platform_errors::{BpError, BpResult};
use std::time::SystemTime;
use time::{Date, Month, OffsetDateTime, Time};

pub(crate) struct AmzDate;

impl AmzDate {
    pub(crate) fn format(now: SystemTime) -> (String, String) {
        let date_time = OffsetDateTime::from(now);
        let date = format!(
            "{:04}{:02}{:02}",
            date_time.year(),
            u8::from(date_time.month()),
            date_time.day()
        );
        let amz_date = format!(
            "{date}T{:02}{:02}{:02}Z",
            date_time.hour(),
            date_time.minute(),
            date_time.second()
        );
        (date, amz_date)
    }

    pub(crate) fn parse(value: &str) -> BpResult<SystemTime> {
        let bytes = value.as_bytes();
        if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
            return Err(Self::invalid("x-amz-date has an invalid format"));
        }
        let year = Self::number(&bytes[0..4])? as i32;
        let month = Self::number(&bytes[4..6])?;
        let day = Self::number(&bytes[6..8])?;
        let hour = Self::number(&bytes[9..11])?;
        let minute = Self::number(&bytes[11..13])?;
        let second = Self::number(&bytes[13..15])?;
        let month = Month::try_from(month as u8).map_err(|_| Self::invalid("invalid month"))?;
        let date = Date::from_calendar_date(year, month, day as u8)
            .map_err(|_| Self::invalid("invalid date"))?;
        let time = Time::from_hms(hour as u8, minute as u8, second as u8)
            .map_err(|_| Self::invalid("invalid time"))?;
        Ok(date.with_time(time).assume_utc().into())
    }

    fn number(bytes: &[u8]) -> BpResult<u32> {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| Self::invalid("x-amz-date has an invalid number"))
    }

    fn invalid(message: impl Into<String>) -> BpError {
        BpError::auth_invalid_jwt(message)
    }
}
