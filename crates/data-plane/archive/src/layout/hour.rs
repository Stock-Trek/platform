use crate::quantized_time::{Quantization, QuantizedTime, TopOfHour};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use platform_types::new_types::TimestampMilliseconds;
use std::str::FromStr;

pub(super) const YEAR_FIELD: &str = "year=";
pub(super) const MONTH_FIELD: &str = "month=";
pub(super) const DAY_FIELD: &str = "day=";
pub(super) const HOUR_FIELD: &str = "hour=";

/// The hourly folders shared by raw and compacted archive objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArchiveHour {
    start: DateTime<Utc>,
}

impl ArchiveHour {
    pub fn from_timestamp(timestamp: TimestampMilliseconds) -> Option<Self> {
        let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp.0)?;
        QuantizedTime::timestamp(timestamp, Quantization::Hour(TopOfHour::One))
            .ok()
            .map(|start| Self { start })
    }

    /// Parse an hourly partition from any path containing the four time fields.
    pub fn parse(path: &str) -> Option<Self> {
        let segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        segments.windows(4).rev().find_map(|window| {
            let year = Self::field::<i32>(window[0], YEAR_FIELD)?;
            let month = Self::field::<u32>(window[1], MONTH_FIELD)?;
            let day = Self::field::<u32>(window[2], DAY_FIELD)?;
            let hour = Self::field::<u32>(window[3], HOUR_FIELD)?;
            Utc.with_ymd_and_hms(year, month, day, hour, 0, 0)
                .single()
                .map(|start| Self { start })
        })
    }

    pub fn bucket_start(&self) -> TimestampMilliseconds {
        TimestampMilliseconds(self.start.timestamp_millis())
    }

    pub fn path(&self) -> String {
        format!(
            "{YEAR_FIELD}{:04}/{MONTH_FIELD}{:02}/{DAY_FIELD}{:02}/{HOUR_FIELD}{:02}",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
        )
    }

    pub fn file_path(&self, file_name: impl AsRef<str>) -> String {
        format!(
            "{}/{}",
            self.path(),
            file_name.as_ref().trim_start_matches('/')
        )
    }

    pub fn year(&self) -> i32 {
        self.start.year()
    }

    pub fn month(&self) -> u32 {
        self.start.month()
    }

    pub fn day(&self) -> u32 {
        self.start.day()
    }

    pub fn hour(&self) -> u32 {
        self.start.hour()
    }

    fn field<T>(segment: &str, name: &str) -> Option<T>
    where
        T: FromStr,
    {
        segment.strip_prefix(name)?.parse().ok()
    }
}
