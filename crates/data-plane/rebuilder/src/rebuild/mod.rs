pub mod candle_spill;
mod rebuild_options;
pub mod replay_spill;

#[cfg(test)]
mod tests;

pub use rebuild_options::*;

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

    fn record(&mut self, path: &Path) -> BpResult<()> {
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

/// Number of runs merged at once, so the merge buffers stay proportional to
/// the configured spill limit.
pub(crate) fn merge_fanin(limit: usize) -> usize {
    (limit / MERGE_BATCH_RECORDS).clamp(2, MAX_MERGE_FANIN)
}
