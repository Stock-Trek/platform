use crate::{
    batch::ArchiveBatch,
    parquet::ArchiveParquetWriter,
    parquet::{ArchiveParquet, ArchiveParquetBatches},
    record::{ArchiveKind, ArchiveRecord},
};
use platform_errors::{BpError, BpResult};
use std::{fs::File, path::Path, vec::IntoIter};

const RUN_BATCH_RECORDS: usize = 8_192;

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

/// Streams records into a spill run file without materializing the run.
pub struct SpillRunWriter {
    kind: ArchiveKind,
    writer: ArchiveParquetWriter<File>,
    buffer: Vec<ArchiveRecord>,
    records: usize,
}

impl SpillRunWriter {
    pub fn create(path: &Path, kind: ArchiveKind) -> BpResult<Self> {
        let file = File::create(path)
            .map_err(|error| BpError::io("create spill run", path.display().to_string(), error))?;
        Ok(Self {
            kind,
            writer: ArchiveParquetWriter::try_new(kind, file)?,
            buffer: Vec::new(),
            records: 0,
        })
    }

    pub fn push(&mut self, record: ArchiveRecord) -> BpResult<()> {
        if record.kind() != self.kind {
            return Err(ArchiveBatch::kind_mismatch(self.kind, &record));
        }
        self.buffer.push(record);
        self.records += 1;
        if self.buffer.len() >= RUN_BATCH_RECORDS {
            self.flush()?;
        }
        Ok(())
    }

    pub fn close(mut self) -> BpResult<()> {
        self.flush()?;
        self.writer.close()
    }

    fn flush(&mut self) -> BpResult<()> {
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
