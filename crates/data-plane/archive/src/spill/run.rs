use crate::{
    batch::ArchiveBatch,
    parquet::ArchiveParquetWriter,
    parquet::{ArchiveParquet, ArchiveParquetBatches},
    record::{ArchiveKind, ArchiveRecord},
};
use platform_errors::{BpError, BpResult};
use std::{fs::File, path::Path, vec::IntoIter};

/// Writes one sorted spill run as a Parquet file.
pub struct SpillRun;

impl SpillRun {
    pub fn write(path: &Path, batch: &ArchiveBatch) -> BpResult<()> {
        let file = File::create(path)
            .map_err(|error| BpError::io("create spill run", path.display().to_string(), error))?;
        let mut writer = ArchiveParquetWriter::try_new(batch.kind(), file)?;
        writer.write(batch)?;
        writer.close()
    }
}

/// Streams the records of one spill run back in written order.
pub struct SpillRunReader {
    batches: ArchiveParquetBatches,
    buffer: IntoIter<ArchiveRecord>,
}

impl SpillRunReader {
    pub fn open(kind: ArchiveKind, path: &Path) -> BpResult<Self> {
        let file = File::open(path)
            .map_err(|error| BpError::io("open spill run", path.display().to_string(), error))?;
        Ok(Self {
            batches: ArchiveParquet::decode_batches(kind, file)?,
            buffer: Vec::new().into_iter(),
        })
    }

    pub fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        loop {
            if let Some(record) = self.buffer.next() {
                return Ok(Some(record));
            }
            match self.batches.next() {
                Some(batch) => self.buffer = batch?.into_records().into_iter(),
                None => return Ok(None),
            }
        }
    }
}
