use platform_errors::{PlatformError, PlatformResult};

pub const DEFAULT_MAX_DATA_FILES: usize = 100;
pub const DEFAULT_MAX_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceStage {
    ExpireSnapshots,
    OrphanCleanup,
    CompactedRewrite,
    RawDeletion,
}

impl MaintenanceStage {
    pub fn all() -> [Self; 4] {
        [
            Self::ExpireSnapshots,
            Self::OrphanCleanup,
            Self::CompactedRewrite,
            Self::RawDeletion,
        ]
    }

    pub fn order(self) -> usize {
        match self {
            Self::ExpireSnapshots => 0,
            Self::OrphanCleanup => 1,
            Self::CompactedRewrite => 2,
            Self::RawDeletion => 3,
        }
    }

    pub fn next(self) -> Option<Self> {
        match self {
            Self::ExpireSnapshots => Some(Self::OrphanCleanup),
            Self::OrphanCleanup => Some(Self::CompactedRewrite),
            Self::CompactedRewrite => Some(Self::RawDeletion),
            Self::RawDeletion => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::ExpireSnapshots => "expire_snapshots",
            Self::OrphanCleanup => "orphan_cleanup",
            Self::CompactedRewrite => "compacted_rewrite",
            Self::RawDeletion => "raw_deletion",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionTarget {
    pub max_data_files: usize,
    pub max_bytes: u64,
}

impl PartitionTarget {
    pub fn validate(self) -> PlatformResult<()> {
        if self.max_data_files == 0 || self.max_bytes == 0 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "maintenance partition targets must be positive",
            )));
        }
        Ok(())
    }

    pub fn exceeded_by(self, files: usize, bytes: u64) -> bool {
        files > self.max_data_files || bytes > self.max_bytes
    }
}

impl Default for PartitionTarget {
    fn default() -> Self {
        Self {
            max_data_files: DEFAULT_MAX_DATA_FILES,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}
