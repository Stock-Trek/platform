use crate::{
    archive_parquet::ArchiveParquetWriter,
    archive_parquet::{ArchiveParquet, ArchiveParquetBatches},
    archive_record::{ArchiveKind, ArchiveRecord},
    batch::ArchiveBatch,
};
use platform_errors::{BuildError, BuildResult};
use std::{fs::File, path::Path, vec::IntoIter};

const RUN_BATCH_RECORDS: usize = 8_192;

/// Writes one sorted spill run as a Parquet file.
pub struct SpillRun;

impl SpillRun {
    pub fn write(path: &Path, batch: &ArchiveBatch) -> BuildResult<()> {
        let file = File::create(path).map_err(|error| {
            BuildError::io("create spill run", path.display().to_string(), error)
        })?;
        let mut writer = ArchiveParquetWriter::try_new(batch.kind(), file)?;
        writer.write(batch)?;
        writer.close()
    }
}

/// Streams records into a spill run file without materializing the run.
///
/// The buffered records are flushed as one Parquet row group per
/// `RUN_BATCH_RECORDS` records, matching the reader's decode batch size.
pub struct SpillRunWriter {
    kind: ArchiveKind,
    writer: ArchiveParquetWriter<File>,
    buffer: Vec<ArchiveRecord>,
    batch_records: usize,
    records: usize,
}

impl SpillRunWriter {
    pub fn create_with_batch_size(
        path: &Path,
        kind: ArchiveKind,
        batch_size: usize,
    ) -> BuildResult<Self> {
        let file = File::create(path).map_err(|error| {
            BuildError::io("create spill run", path.display().to_string(), error)
        })?;
        Ok(Self {
            kind,
            writer: ArchiveParquetWriter::try_new(kind, file)?,
            buffer: Vec::new(),
            batch_records: batch_size.max(1),
            records: 0,
        })
    }

    pub fn push(&mut self, record: ArchiveRecord) -> BuildResult<()> {
        if record.kind() != self.kind {
            return Err(ArchiveBatch::kind_mismatch(self.kind, &record));
        }
        self.buffer.push(record);
        self.records += 1;
        if self.buffer.len() >= self.batch_records {
            self.flush()?;
        }
        Ok(())
    }

    pub fn close(mut self) -> BuildResult<()> {
        self.flush()?;
        self.writer.close()
    }

    fn flush(&mut self) -> BuildResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let records = std::mem::take(&mut self.buffer);
        let batch = ArchiveBatch::new(self.kind, records)?;
        self.writer.write(&batch)
    }
}

/// Streams the records of one spill run back in written order.
pub struct SpillRunReader {
    batches: ArchiveParquetBatches,
    buffer: IntoIter<ArchiveRecord>,
}

impl SpillRunReader {
    pub fn open(kind: ArchiveKind, path: &Path) -> BuildResult<Self> {
        Self::open_with_batch_size(kind, path, RUN_BATCH_RECORDS)
    }

    pub fn open_with_batch_size(
        kind: ArchiveKind,
        path: &Path,
        batch_size: usize,
    ) -> BuildResult<Self> {
        let file = File::open(path)
            .map_err(|error| BuildError::io("open spill run", path.display().to_string(), error))?;
        Ok(Self {
            batches: ArchiveParquet::decode_batches_with_batch_size(kind, file, batch_size)?,
            buffer: Vec::new().into_iter(),
        })
    }

    pub fn next_record(&mut self) -> BuildResult<Option<ArchiveRecord>> {
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
