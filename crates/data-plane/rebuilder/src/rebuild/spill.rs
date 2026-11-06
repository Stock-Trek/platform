use platform_errors::{BpError, BpResult};
use std::path::Path;

const MERGE_BATCH_RECORDS: usize = 8_192;
const MAX_MERGE_FANIN: usize = 64;

/// Bytes written to spill run files and the largest single run.
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

    pub(crate) fn record(&mut self, path: &Path) -> BpResult<()> {
        let bytes = std::fs::metadata(path)
            .map_err(|error| {
                BpError::io(
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

pub(crate) struct SpillMerge;

impl SpillMerge {
    /// Number of runs merged at once, so the merge buffers stay proportional
    /// to the configured spill limit.
    pub(crate) fn fanin(limit: usize) -> usize {
        (limit / MERGE_BATCH_RECORDS).clamp(2, MAX_MERGE_FANIN)
    }

    /// Records decoded or buffered per run during a merge, so the merge
    /// buffers never exceed the configured spill limit even for tiny limits.
    pub(crate) fn batch_records(limit: usize) -> usize {
        (limit / Self::fanin(limit)).clamp(1, MERGE_BATCH_RECORDS)
    }
}
