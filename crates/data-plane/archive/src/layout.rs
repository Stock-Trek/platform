//! Shared S3 layout for raw and compacted archive objects.
//!
//! Both services build keys through this module so the hourly partitioning and
//! prefix handling cannot drift apart.

use crate::{
    ArchiveKind, ArchivePartition, ArchiveRecord,
    quantized_time::{Quantization, QuantizedTime, TopOfHour},
    routing::RoutingBucket,
};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use platform_errors::{BpError, BpResult, DataError};
use platform_types::new_types::TimestampMilliseconds;
use std::str::FromStr;

const YEAR_FIELD: &str = "year=";
const MONTH_FIELD: &str = "month=";
const DAY_FIELD: &str = "day=";
const HOUR_FIELD: &str = "hour=";

/// The single place where archive object keys are built and parsed.
pub struct ArchivePath;

impl ArchivePath {
    /// The start of the bucket that contains `timestamp`.
    pub fn bucket_start(
        timestamp: TimestampMilliseconds,
        bucket_millis: i64,
    ) -> TimestampMilliseconds {
        debug_assert!(
            bucket_millis > 0,
            "archive bucket size must be greater than zero"
        );
        TimestampMilliseconds(timestamp.0.div_euclid(bucket_millis) * bucket_millis)
    }

    /// Build a raw archive object key.
    ///
    /// Raw files are grouped by kind, then by a fixed virtual routing bucket
    /// and hour:
    /// `{prefix}/{kind}/bucket=.../year=.../month=.../day=.../hour=.../{file}`.
    pub fn raw_key(
        prefix: &str,
        kind: ArchiveKind,
        bucket_start: TimestampMilliseconds,
        routing_bucket: RoutingBucket,
        file_name: impl AsRef<str>,
    ) -> BpResult<String> {
        let hour = Self::hour(bucket_start, "build archive object key")?;
        Ok(Self::join(
            prefix,
            &format!(
                "{}/{}",
                Self::raw_partition(kind, routing_bucket),
                hour.file_path(file_name)
            ),
        ))
    }

    /// Build a compacted archive object key.
    ///
    /// Compacted files keep the symbol partition, add the same virtual routing
    /// bucket, and then the hourly folders:
    /// `{prefix}/{partition}/bucket=.../year=.../month=.../day=.../hour=.../{file}`.
    pub fn compacted_key(
        prefix: &str,
        partition: &ArchivePartition,
        bucket_start: TimestampMilliseconds,
        routing_bucket: RoutingBucket,
        file_name: impl AsRef<str>,
    ) -> BpResult<String> {
        let hour = Self::hour(bucket_start, "build compacted object key")?;
        Ok(Self::join(
            prefix,
            &format!(
                "{}/{}",
                Self::compacted_partition(partition, routing_bucket),
                hour.file_path(file_name)
            ),
        ))
    }

    /// The routing-bucket prefix shared by all raw objects of a kind.
    pub fn raw_partition(kind: ArchiveKind, routing_bucket: RoutingBucket) -> String {
        format!("{}/{}", kind.label(), routing_bucket.segment())
    }

    /// The routing-bucket prefix shared by all compacted objects of a symbol.
    pub fn compacted_partition(
        partition: &ArchivePartition,
        routing_bucket: RoutingBucket,
    ) -> String {
        format!("{}/{}", partition.path(), routing_bucket.segment())
    }

    /// Join a prefix and a relative path without duplicate separators.
    pub fn join(prefix: &str, path: &str) -> String {
        let prefix = prefix.trim_matches('/');
        let path = path.trim_matches('/');
        match (prefix.is_empty(), path.is_empty()) {
            (true, _) => path.to_string(),
            (false, true) => prefix.to_string(),
            (false, false) => format!("{prefix}/{path}"),
        }
    }

    /// Normalize and validate a requested compaction target.
    ///
    /// The target may name a raw or compacted hour folder, or a Parquet file
    /// inside one. Paths must contain a known archive kind and a complete set
    /// of `year=`, `month=`, `day=`, and `hour=` fields after the kind/partition
    /// prefix. The returned prefix always ends in a slash.
    pub fn compaction_prefix(target: &str) -> BpResult<String> {
        let target = target.trim();
        if target.is_empty() {
            return Err(BpError::Data(DataError::InvalidJob {
                message: "compact target cannot be empty".to_string(),
            }));
        }
        let is_file_target = !target.ends_with('/');
        let target = target.trim_matches('/');
        let mut segments = target
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        if segments
            .iter()
            .any(|segment| matches!(*segment, "." | ".."))
        {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!("compact target {target} contains an invalid path segment"),
            }));
        }
        if is_file_target
            && segments
                .last()
                .is_some_and(|segment| segment.ends_with(".parquet"))
        {
            segments.pop();
        }
        if segments
            .iter()
            .position(|segment| segment.starts_with(YEAR_FIELD))
            .is_none_or(|index| {
                index == 0 || segments.len() < index + 4 || !Self::is_hour_path(&segments[index..])
            })
        {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!(
                    "compact target {target} must end with an hourly archive partition"
                ),
            }));
        }
        if Self::kind(&segments[..segments.len() - 4].join("/")).is_none() {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!("compact target {target} must include an archive kind"),
            }));
        }
        Ok(format!("{}/", segments.join("/")))
    }

    fn is_hour_path(segments: &[&str]) -> bool {
        segments.len() == 4
            && segments[0].starts_with(YEAR_FIELD)
            && segments[1].starts_with(MONTH_FIELD)
            && segments[2].starts_with(DAY_FIELD)
            && segments[3].starts_with(HOUR_FIELD)
            && ArchiveHour::parse(&segments.join("/")).is_some()
    }

    /// Remove a configured prefix from an object key.
    pub fn relative_key<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
        let key = key.trim_start_matches('/');
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            return Some(key);
        }
        let relative = key.strip_prefix(prefix)?;
        if relative.is_empty() {
            Some(relative)
        } else {
            relative.strip_prefix('/')
        }
    }

    /// The archive kind named by the first path segment.
    pub fn kind(path: &str) -> Option<ArchiveKind> {
        path.split('/').find_map(ArchiveKind::from_segment)
    }

    /// Parse the hourly bucket start from an object key or partition path.
    pub fn bucket_start_from_key(path: &str) -> Option<TimestampMilliseconds> {
        ArchiveHour::parse(path).map(|hour| hour.bucket_start())
    }

    /// Parse the virtual routing bucket from an object key or partition path.
    pub fn routing_bucket_from_key(path: &str) -> Option<RoutingBucket> {
        RoutingBucket::parse(path)
    }

    /// Parse both the kind and the hourly bucket from an object key.
    pub fn parse_key(path: &str) -> Option<ArchiveBucket> {
        Some(ArchiveBucket::from_parts(
            Self::kind(path)?,
            Self::bucket_start_from_key(path)?,
        ))
    }

    fn hour(bucket_start: TimestampMilliseconds, operation: &'static str) -> BpResult<ArchiveHour> {
        ArchiveHour::from_timestamp(bucket_start)
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange { operation }))
    }
}

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

/// A record kind and the hourly bucket it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchiveBucket {
    pub kind: ArchiveKind,
    pub bucket_start: TimestampMilliseconds,
}

impl ArchiveBucket {
    pub fn new(record: &ArchiveRecord, bucket_millis: i64) -> Self {
        Self::from_parts(
            record.kind(),
            ArchivePath::bucket_start(record.timestamp(), bucket_millis),
        )
    }

    pub fn from_parts(kind: ArchiveKind, bucket_start: TimestampMilliseconds) -> Self {
        Self { kind, bucket_start }
    }

    pub fn raw_key(
        &self,
        prefix: &str,
        routing_bucket: RoutingBucket,
        file_name: impl AsRef<str>,
    ) -> BpResult<String> {
        ArchivePath::raw_key(
            prefix,
            self.kind,
            self.bucket_start,
            routing_bucket,
            file_name,
        )
    }

    pub fn compacted_key(
        &self,
        prefix: &str,
        partition: &ArchivePartition,
        routing_bucket: RoutingBucket,
        file_name: impl AsRef<str>,
    ) -> BpResult<String> {
        ArchivePath::compacted_key(
            prefix,
            partition,
            self.bucket_start,
            routing_bucket,
            file_name,
        )
    }

    pub fn parse(path: &str) -> Option<Self> {
        ArchivePath::parse_key(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_market_data::candle::CandleInterval;
    use platform_market_data::candle::CandleKey;
    use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

    fn timestamp(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .timestamp_millis()
    }

    fn event_partition() -> ArchivePartition {
        ArchivePartition::Events {
            exchange: CexId::Binance,
            stream: "btc/usdt@trade".to_string(),
        }
    }

    fn candle_partition() -> ArchivePartition {
        ArchivePartition::Candles {
            candle: CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                CandleInterval::from_label("1h").unwrap(),
            ),
        }
    }

    #[test]
    fn buckets_are_quantized_to_their_start() {
        let start = timestamp(2024, 1, 2, 3, 0);
        let within = TimestampMilliseconds(start + 37 * 60 * 1_000);
        assert_eq!(
            ArchivePath::bucket_start(within, 60 * 60 * 1_000),
            TimestampMilliseconds(start)
        );
        assert_eq!(
            ArchivePath::bucket_start(TimestampMilliseconds(-1), 60 * 60 * 1_000),
            TimestampMilliseconds(-60 * 60 * 1_000)
        );
        assert_eq!(
            ArchivePath::bucket_start(within, 5 * 60 * 1_000),
            TimestampMilliseconds(timestamp(2024, 1, 2, 3, 35))
        );
    }

    #[test]
    fn archive_hours_are_quantized_to_the_top_of_the_hour() {
        let start = timestamp(2024, 1, 2, 3, 0);
        let within = TimestampMilliseconds(start + 37 * 60 * 1_000 + 123);

        assert_eq!(
            ArchiveHour::from_timestamp(within).unwrap().bucket_start(),
            TimestampMilliseconds(start)
        );
    }

    #[test]
    fn raw_and_compacted_keys_share_the_hourly_layout() {
        let bucket = TimestampMilliseconds(timestamp(2024, 1, 2, 3, 0));
        let routing_bucket = RoutingBucket::from_index(7).unwrap();

        assert_eq!(
            ArchivePath::raw_key(
                "market-data/raw/",
                ArchiveKind::Events,
                bucket,
                routing_bucket,
                "file.parquet"
            )
            .unwrap(),
            "market-data/raw/events/bucket=00007/year=2024/month=01/day=02/hour=03/file.parquet"
        );
        assert_eq!(
            ArchivePath::compacted_key(
                "/market-data/compacted/",
                &event_partition(),
                bucket,
                routing_bucket,
                "/file.parquet"
            )
            .unwrap(),
            "market-data/compacted/kind=events/exchange=binance/stream=btc_usdt@trade/bucket=00007/year=2024/month=01/day=02/hour=03/file.parquet"
        );
        assert_eq!(
            ArchivePath::compacted_key(
                "/",
                &candle_partition(),
                bucket,
                routing_bucket,
                "data.parquet"
            )
            .unwrap(),
            "kind=candles/exchange=binance/base=bitcoin/quote=tether_usd/interval=1h/bucket=00007/year=2024/month=01/day=02/hour=03/data.parquet"
        );
    }

    #[test]
    fn keys_parse_back_to_the_same_bucket() {
        let bucket = TimestampMilliseconds(timestamp(2024, 1, 2, 3, 0));
        let routing_bucket = RoutingBucket::from_index(42).unwrap();
        let raw = ArchivePath::raw_key(
            "market-data/raw",
            ArchiveKind::Candles,
            bucket,
            routing_bucket,
            "file.parquet",
        )
        .unwrap();
        let compacted = ArchivePath::compacted_key(
            "market-data/compacted",
            &candle_partition(),
            bucket,
            routing_bucket,
            "data.parquet",
        )
        .unwrap();

        for key in [raw, compacted] {
            let parsed = ArchivePath::parse_key(&key).unwrap();
            assert_eq!(parsed.kind, ArchiveKind::Candles);
            assert_eq!(parsed.bucket_start, bucket);
            assert_eq!(
                ArchivePath::routing_bucket_from_key(&key),
                Some(routing_bucket)
            );
            assert_eq!(
                ArchiveHour::parse(&key).unwrap().path(),
                "year=2024/month=01/day=02/hour=03"
            );
        }
    }

    #[test]
    fn keys_can_be_made_relative_to_a_configured_prefix() {
        let key = "market-data/raw/events/year=2024/month=01/day=02/hour=03/file.parquet";
        let relative = ArchivePath::relative_key(key, "market-data/raw/").unwrap();
        assert_eq!(
            relative,
            "events/year=2024/month=01/day=02/hour=03/file.parquet"
        );
        assert_eq!(ArchivePath::relative_key(key, "").unwrap(), key);
        assert_eq!(
            ArchivePath::relative_key("other/key", "market-data/raw"),
            None
        );
    }

    #[test]
    fn invalid_hourly_paths_are_rejected() {
        assert!(ArchiveHour::parse("events/year=2024/month=13/day=01/hour=03").is_none());
        assert!(ArchiveHour::parse("events/year=2024/month=01/day=32/hour=03").is_none());
        assert!(ArchiveHour::parse("events/year=2024/month=01/day=01/hour=24").is_none());
        assert!(ArchiveHour::parse("events/year=2024/month=01/day=01").is_none());
    }

    #[test]
    fn compaction_targets_are_normalized_to_hour_prefixes() {
        assert_eq!(
            ArchivePath::compaction_prefix(
                "market-data/raw/events/year=2024/month=01/day=02/hour=03/"
            )
            .unwrap(),
            "market-data/raw/events/year=2024/month=01/day=02/hour=03/"
        );
        assert_eq!(
            ArchivePath::compaction_prefix(
                "market-data/compacted/kind=candles/exchange=binance/base=bitcoin/quote=tether_usd/interval=1h/year=2024/month=01/day=02/hour=03/data.parquet"
            )
            .unwrap(),
            "market-data/compacted/kind=candles/exchange=binance/base=bitcoin/quote=tether_usd/interval=1h/year=2024/month=01/day=02/hour=03/"
        );
    }

    #[test]
    fn invalid_compaction_targets_are_rejected() {
        assert!(ArchivePath::compaction_prefix("").is_err());
        assert!(
            ArchivePath::compaction_prefix("market-data/raw/events/year=2024/month=01/day=02")
                .is_err()
        );
        assert!(
            ArchivePath::compaction_prefix(
                "market-data/raw/unknown/year=2024/month=01/day=02/hour=03/"
            )
            .is_err()
        );
        assert!(
            ArchivePath::compaction_prefix(
                "market-data/raw/events/../../year=2024/month=01/day=02/hour=03/"
            )
            .is_err()
        );
    }
}
