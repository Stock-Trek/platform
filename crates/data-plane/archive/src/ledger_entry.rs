use crate::{ArchiveKind, CompactionId, SourceManifest};
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;

pub const LEDGER_FORMAT_VERSION: u32 = 1;
pub const HOUR_MILLIS: i64 = 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    pub format_version: u32,
    pub compaction_id: CompactionId,
    pub kind: ArchiveKind,
    pub partition_hour_millis: i64,
    pub source_manifest_uri: String,
    pub source_count: i64,
    pub processed_at: TimestampMilliseconds,
}

impl LedgerEntry {
    pub fn new(
        compaction_id: CompactionId,
        kind: ArchiveKind,
        bucket_start: TimestampMilliseconds,
        source_manifest_uri: String,
        source_count: i64,
        processed_at: TimestampMilliseconds,
    ) -> PlatformResult<Self> {
        let entry = Self {
            format_version: LEDGER_FORMAT_VERSION,
            compaction_id,
            kind,
            partition_hour_millis: Self::hour_start(bucket_start).0,
            source_manifest_uri,
            source_count,
            processed_at,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn from_manifest(
        kind: ArchiveKind,
        bucket_start: TimestampMilliseconds,
        source_manifest_uri: String,
        manifest: &SourceManifest,
        processed_at: TimestampMilliseconds,
    ) -> PlatformResult<Self> {
        manifest.validate()?;
        let source_count = i64::try_from(manifest.source_count()).map_err(|error| {
            PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("source manifest is too large: {error}"),
            ))
        })?;
        Self::new(
            manifest.compaction_id,
            kind,
            bucket_start,
            source_manifest_uri,
            source_count,
            processed_at,
        )
    }

    pub fn validate(&self) -> PlatformResult<()> {
        if self.format_version == 0 || self.format_version > LEDGER_FORMAT_VERSION {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported ledger format version {}", self.format_version),
            )));
        }
        if self.source_manifest_uri.is_empty() {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ledger entry has no source manifest URI",
            )));
        }
        if self.source_count <= 0 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ledger entry source count must be positive",
            )));
        }
        if self.partition_hour_millis.rem_euclid(HOUR_MILLIS) != 0 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ledger partition hour is not aligned to an hour",
            )));
        }
        Ok(())
    }

    pub fn hour_start(timestamp: TimestampMilliseconds) -> TimestampMilliseconds {
        TimestampMilliseconds(timestamp.0.div_euclid(HOUR_MILLIS) * HOUR_MILLIS)
    }

    pub fn partition_hour(&self) -> TimestampMilliseconds {
        TimestampMilliseconds(self.partition_hour_millis)
    }

    pub fn is_older_than(
        &self,
        now: TimestampMilliseconds,
        retention: std::time::Duration,
    ) -> bool {
        let retention_millis = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
        self.processed_at.0.saturating_add(retention_millis) <= now.0
    }
}
