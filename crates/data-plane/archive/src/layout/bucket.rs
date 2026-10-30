use super::ArchivePath;
use crate::{ArchiveKind, ArchivePartition, ArchiveRecord, routing::RoutingBucket};
use platform_errors::BpResult;
use platform_types::new_types::TimestampMilliseconds;

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
