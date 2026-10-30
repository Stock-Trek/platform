//! Shared S3 layout for raw and compacted archive objects.
//!
//! Both services build keys through this module so the hourly partitioning and
//! prefix handling cannot drift apart.

use super::hour::{ArchiveHour, DAY_FIELD, HOUR_FIELD, MONTH_FIELD, YEAR_FIELD};
use crate::{ArchiveKind, ArchivePartition, routing::RoutingBucket};
use platform_errors::{BpError, BpResult, DataError};
use platform_types::new_types::TimestampMilliseconds;

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
    pub fn parse_key(path: &str) -> Option<super::ArchiveBucket> {
        Some(super::ArchiveBucket::from_parts(
            Self::kind(path)?,
            Self::bucket_start_from_key(path)?,
        ))
    }

    fn hour(bucket_start: TimestampMilliseconds, operation: &'static str) -> BpResult<ArchiveHour> {
        ArchiveHour::from_timestamp(bucket_start)
            .ok_or(BpError::Data(DataError::DateTimeOutOfRange { operation }))
    }
}
