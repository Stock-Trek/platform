use crate::ArchiveKind;
use crate::maintenance_stage::PartitionTarget;
use platform_errors::{PlatformError, PlatformResult};
use std::{collections::BTreeMap, time::Duration};

pub const MIN_RAW_RETENTION: Duration = Duration::from_secs(90 * 24 * 60 * 60);
pub const MIN_COMPACTED_SNAPSHOTS: usize = 10;
pub const MIN_SNAPSHOT_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
pub const MIN_ORPHAN_GRACE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_RAW_RETENTION: Duration = Duration::from_secs(365 * 24 * 60 * 60);
pub const DEFAULT_SNAPSHOT_RETENTION: Duration = Duration::from_secs(90 * 24 * 60 * 60);
pub const DEFAULT_ORPHAN_GRACE: Duration = Duration::from_secs(14 * 24 * 60 * 60);
pub const DEFAULT_MAX_COMMITS_PER_HOUR: u64 = 120;
pub const DEFAULT_MAX_REWRITE_BYTES_PER_HOUR: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceConfig {
    pub raw_retention: BTreeMap<ArchiveKind, Duration>,
    pub snapshot_retention: Duration,
    pub snapshots_to_keep: usize,
    pub orphan_grace: Duration,
    pub longest_lease: Duration,
    pub compacted_target: PartitionTarget,
    pub ledger_target: PartitionTarget,
    pub max_rewrite_commits_per_partition: usize,
    pub max_metadata_growth_percent: u8,
    pub max_commits_per_hour: u64,
    pub max_rewrite_bytes_per_hour: u64,
}

impl MaintenanceConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_raw_retention(
        mut self,
        kind: ArchiveKind,
        retention: Duration,
    ) -> PlatformResult<Self> {
        self.raw_retention.insert(kind, retention);
        self.validate()?;
        Ok(self)
    }

    pub fn with_snapshot_retention(mut self, retention: Duration) -> PlatformResult<Self> {
        self.snapshot_retention = retention;
        self.validate()?;
        Ok(self)
    }

    pub fn with_orphan_grace(mut self, grace: Duration) -> PlatformResult<Self> {
        self.orphan_grace = grace;
        self.validate()?;
        Ok(self)
    }

    pub fn raw_retention_for(&self, kind: ArchiveKind) -> PlatformResult<Duration> {
        self.raw_retention.get(&kind).copied().ok_or_else(|| {
            PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("raw retention is not configured for {}", kind.label()),
            ))
        })
    }

    pub fn validate(&self) -> PlatformResult<()> {
        for kind in [ArchiveKind::Events, ArchiveKind::Candles] {
            let retention = self.raw_retention_for(kind)?;
            if retention < MIN_RAW_RETENTION {
                return Err(Self::floor_error(
                    "raw retention",
                    retention,
                    MIN_RAW_RETENTION,
                ));
            }
        }
        if self.snapshot_retention < MIN_SNAPSHOT_RETENTION {
            return Err(Self::floor_error(
                "snapshot retention",
                self.snapshot_retention,
                MIN_SNAPSHOT_RETENTION,
            ));
        }
        if self.snapshots_to_keep < MIN_COMPACTED_SNAPSHOTS {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "snapshot count {} is below the floor of {}",
                    self.snapshots_to_keep, MIN_COMPACTED_SNAPSHOTS
                ),
            )));
        }
        if self.orphan_grace < MIN_ORPHAN_GRACE {
            return Err(Self::floor_error(
                "orphan grace",
                self.orphan_grace,
                MIN_ORPHAN_GRACE,
            ));
        }
        if self.orphan_grace <= self.longest_lease {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "orphan grace must exceed the longest maintenance or write lease",
            )));
        }
        self.compacted_target.validate()?;
        self.ledger_target.validate()?;
        if self.max_rewrite_commits_per_partition != 1 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "maintenance permits exactly one rewrite commit per partition per cycle",
            )));
        }
        if self.max_metadata_growth_percent == 0 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "metadata growth alert threshold must be positive",
            )));
        }
        if self.max_commits_per_hour == 0 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "maintenance commit budget per hour must be positive",
            )));
        }
        if self.max_rewrite_bytes_per_hour == 0 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "maintenance byte budget per hour must be positive",
            )));
        }
        Ok(())
    }

    fn floor_error(name: &str, value: Duration, floor: Duration) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{name} {}s is below the floor of {}s",
                value.as_secs(),
                floor.as_secs()
            ),
        ))
    }
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            raw_retention: BTreeMap::from([
                (ArchiveKind::Events, DEFAULT_RAW_RETENTION),
                (ArchiveKind::Candles, DEFAULT_RAW_RETENTION),
            ]),
            snapshot_retention: DEFAULT_SNAPSHOT_RETENTION,
            snapshots_to_keep: MIN_COMPACTED_SNAPSHOTS,
            orphan_grace: DEFAULT_ORPHAN_GRACE,
            longest_lease: Duration::from_secs(5 * 60),
            compacted_target: PartitionTarget::default(),
            ledger_target: PartitionTarget::default(),
            max_rewrite_commits_per_partition: 1,
            max_metadata_growth_percent: 10,
            max_commits_per_hour: DEFAULT_MAX_COMMITS_PER_HOUR,
            max_rewrite_bytes_per_hour: DEFAULT_MAX_REWRITE_BYTES_PER_HOUR,
        }
    }
}

pub type RetentionPolicy = MaintenanceConfig;
pub type MaintenanceRetention = MaintenanceConfig;
pub type PartitionTargets = PartitionTarget;
