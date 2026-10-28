use crate::{
    batch::ArchiveBatch, parquet_reader::ParquetReader, parquet_writer::writer_properties,
    record::ArchiveKind,
};
use bytes::Bytes;
use futures::StreamExt;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        async_reader::ParquetRecordBatchStreamBuilder,
    },
    file::reader::ChunkReader,
};
use platform_errors::{BpError, BpResult};
use std::io::Write;

const WRITE_BATCH_SIZE: usize = 8_192;

/// Reads and writes the Parquet format shared by the archiver and compactor.
pub struct ArchiveParquet;

/// Streams decoded archive batches one bounded batch at a time.
pub struct ArchiveParquetBatches {
    kind: ArchiveKind,
    reader: ParquetRecordBatchReader,
}

impl Iterator for ArchiveParquetBatches {
    type Item = BpResult<ArchiveBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next()? {
            Ok(batch) => Some(ArchiveBatch::from_record_batch(self.kind, &batch)),
            Err(error) => Some(Err(error.into())),
        }
    }
}

/// Streams encoded archive records into a writer without building the whole
/// Parquet file in memory.
pub struct ArchiveParquetWriter<W: Write + Send> {
    kind: ArchiveKind,
    writer: ArrowWriter<W>,
}

impl<W: Write + Send> ArchiveParquetWriter<W> {
    pub fn try_new(kind: ArchiveKind, sink: W) -> BpResult<Self> {
        let writer =
            ArrowWriter::try_new(sink, ArchiveBatch::schema(kind), Some(writer_properties()))?;
        Ok(Self { kind, writer })
    }

    pub fn write(&mut self, batch: &ArchiveBatch) -> BpResult<()> {
        if batch.kind() != self.kind {
            return Err(Self::kind_mismatch(self.kind, batch.kind()));
        }
        for chunk in batch.records().chunks(WRITE_BATCH_SIZE) {
            self.writer
                .write(&ArchiveBatch::record_batch(self.kind, chunk)?)?;
        }
        Ok(())
    }

    pub fn close(self) -> BpResult<()> {
        self.writer.close()?;
        Ok(())
    }

    fn kind_mismatch(writer: ArchiveKind, batch: ArchiveKind) -> BpError {
        BpError::io(
            "write archived parquet",
            writer.label(),
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "batch kind {} does not match writer kind {}",
                    batch.label(),
                    writer.label()
                ),
            ),
        )
    }
}

impl ArchiveParquet {
    pub fn encode(batch: &ArchiveBatch) -> BpResult<Vec<u8>> {
        let batch = batch.to_record_batch()?;
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(writer_properties()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buffer)
    }

    pub fn decode(kind: ArchiveKind, bytes: Bytes) -> BpResult<ArchiveBatch> {
        let mut records = Vec::new();
        for batch in Self::decode_batches(kind, bytes)? {
            records.extend(batch?.into_records());
        }
        ArchiveBatch::new(kind, records)
    }

    /// Decodes a Parquet source in bounded batches. The source can be an
    /// in-memory buffer or a file, so callers can avoid materializing both the
    /// encoded object and every decoded record at once.
    pub fn decode_batches<R>(kind: ArchiveKind, source: R) -> BpResult<ArchiveParquetBatches>
    where
        R: ChunkReader + 'static,
    {
        let builder = ParquetRecordBatchReaderBuilder::try_new(source)?;
        let reader = builder.with_batch_size(8_192).build()?;
        Ok(ArchiveParquetBatches { kind, reader })
    }

    /// Decode an object without downloading it in full.
    ///
    /// The reader fetches the Parquet footer/metadata and only the column chunks
    /// needed for each record batch through the S3 transfer manager.
    pub async fn decode_reader(kind: ArchiveKind, reader: ParquetReader) -> BpResult<ArchiveBatch> {
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let mut stream = builder.with_batch_size(8_192).build()?;
        let mut records = Vec::new();
        while let Some(batch) = stream.next().await {
            records.extend(ArchiveBatch::from_record_batch(kind, &batch?)?.into_records());
        }
        ArchiveBatch::new(kind, records)
    }
}
