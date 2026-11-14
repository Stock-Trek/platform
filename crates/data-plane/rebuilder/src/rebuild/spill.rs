use platform_errors::{PlatformError, PlatformResult};
use std::path::Path;

const MERGE_BATCH_RECORDS: usize = 8_192;
const MAX_MERGE_FANIN: usize = 64;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SpillUsage {
    pub bytes_written: u64,
    pub largest_run_bytes: u64,
}

impl SpillUsage {
    pub fn add(&mut self, other: Self) {
        self.bytes_written += other.bytes_written;
        self.largest_run_bytes = self.largest_run_bytes.max(other.largest_run_bytes);
    }

    pub fn record(&mut self, path: &Path) -> PlatformResult<()> {
        let bytes = std::fs::metadata(path)
            .map_err(|error| {
                PlatformError::io(
                    "measure rebuild spill run",
                    path.display().to_string(),
                    error,
                )
            })?
            .len();
        self.bytes_written += bytes;
        self.largest_run_bytes = self.largest_run_bytes.max(bytes);
        Ok(())
    }
}

pub struct SpillMergeLimits;

impl SpillMergeLimits {
    pub fn fanin(limit: usize) -> usize {
        (limit / MERGE_BATCH_RECORDS).clamp(2, MAX_MERGE_FANIN)
    }

    pub fn batch_records(limit: usize) -> usize {
        (limit / Self::fanin(limit)).clamp(1, MERGE_BATCH_RECORDS)
    }
}
