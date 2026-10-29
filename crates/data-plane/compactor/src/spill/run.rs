use platform_archive::{ArchiveBatch, ArchiveParquetWriter};
use platform_errors::{BpError, BpResult};
use std::{fs::File, path::Path};

pub struct SpillRun;

impl SpillRun {
    pub fn write(path: &Path, batch: &ArchiveBatch) -> BpResult<()> {
        let file = File::create(path).map_err(|error| {
            BpError::io(
                "create compaction spill run",
                path.display().to_string(),
                error,
            )
        })?;
        let mut writer = ArchiveParquetWriter::try_new(batch.kind(), file)?;
        writer.write(batch)?;
        writer.close()
    }
}
