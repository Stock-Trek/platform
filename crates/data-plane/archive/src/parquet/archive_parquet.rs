use crate::{batch::ArchiveBatch, parquet_reader::ParquetReader, record::ArchiveKind};
use bytes::Bytes;
use futures::StreamExt;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        async_reader::{
            AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
        },
    },
    basic::Compression,
    file::{properties::WriterProperties, reader::ChunkReader},
};
use platform_errors::{BuildError, BuildResult};
use std::io::Write;

const READ_BATCH_SIZE: usize = 8_192;
const WRITE_BATCH_SIZE: usize = 8_192;

/// Reads and writes the Parquet format shared by the archiver and compactor.
pub struct ArchiveParquet;

/// Streams decoded archive batches one bounded batch at a time.
pub struct ArchiveParquetBatches {
    kind: ArchiveKind,
    reader: ParquetRecordBatchReader,
}

impl Iterator for ArchiveParquetBatches {
    type Item = BuildResult<ArchiveBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next()? {
            Ok(batch) => Some(ArchiveBatch::from_record_batch(self.kind, &batch)),
            Err(error) => Some(Err(error.into())),
        }
    }
}

/// Streams decoded archive batches from an asynchronous Parquet source.
///
/// The stream fetches metadata and column chunks on demand, so callers can
/// process an object without downloading or decoding it in full. Only one
/// bounded batch of records is materialized at a time.
pub struct ArchiveParquetStream<T> {
    kind: ArchiveKind,
    stream: ParquetRecordBatchStream<T>,
}

impl<T> ArchiveParquetStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    pub async fn new(kind: ArchiveKind, reader: T) -> BuildResult<Self> {
        Self::with_batch_size(kind, reader, READ_BATCH_SIZE).await
    }

    pub(super) async fn with_batch_size(
        kind: ArchiveKind,
        reader: T,
        batch_size: usize,
    ) -> BuildResult<Self> {
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let stream = builder.with_batch_size(batch_size).build()?;
        Ok(Self { kind, stream })
    }

    /// Returns the next bounded batch, or `None` when the object is exhausted.
    pub async fn next_batch(&mut self) -> BuildResult<Option<ArchiveBatch>> {
        match self.stream.next().await {
            Some(Ok(batch)) => Ok(Some(ArchiveBatch::from_record_batch(self.kind, &batch)?)),
            Some(Err(error)) => Err(error.into()),
            None => Ok(None),
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
    pub fn try_new(kind: ArchiveKind, sink: W) -> BuildResult<Self> {
        let writer = ArrowWriter::try_new(
            sink,
            ArchiveBatch::schema(kind),
            Some(ArchiveParquet::writer_properties()),
        )?;
        Ok(Self { kind, writer })
    }

    pub fn write(&mut self, batch: &ArchiveBatch) -> BuildResult<()> {
        if batch.kind() != self.kind {
            return Err(Self::kind_mismatch(self.kind, batch.kind()));
        }
        for chunk in batch.records().chunks(WRITE_BATCH_SIZE) {
            self.writer
                .write(&ArchiveBatch::record_batch(self.kind, chunk)?)?;
        }
        Ok(())
    }

    pub fn close(self) -> BuildResult<()> {
        self.writer.close()?;
        Ok(())
    }

    fn kind_mismatch(writer: ArchiveKind, batch: ArchiveKind) -> BuildError {
        BuildError::io(
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
    pub fn encode(batch: &ArchiveBatch) -> BuildResult<Vec<u8>> {
        let batch = batch.to_record_batch()?;
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(Self::writer_properties()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buffer)
    }

    pub fn decode(kind: ArchiveKind, bytes: Bytes) -> BuildResult<ArchiveBatch> {
        let mut records = Vec::new();
        for batch in Self::decode_batches(kind, bytes)? {
            records.extend(batch?.into_records());
        }
        ArchiveBatch::new(kind, records)
    }

    /// Decodes a Parquet source in bounded batches. The source can be an
    /// in-memory buffer or a file, so callers can avoid materializing both the
    /// encoded object and every decoded record at once.
    pub fn decode_batches<R>(kind: ArchiveKind, source: R) -> BuildResult<ArchiveParquetBatches>
    where
        R: ChunkReader + 'static,
    {
        Self::decode_batches_with_batch_size(kind, source, READ_BATCH_SIZE)
    }

    /// Decodes a Parquet source in batches of at most `batch_size` records, so
    /// callers can keep the decoded records under an explicit memory budget.
    pub fn decode_batches_with_batch_size<R>(
        kind: ArchiveKind,
        source: R,
        batch_size: usize,
    ) -> BuildResult<ArchiveParquetBatches>
    where
        R: ChunkReader + 'static,
    {
        let builder = ParquetRecordBatchReaderBuilder::try_new(source)?;
        let reader = builder.with_batch_size(batch_size.max(1)).build()?;
        Ok(ArchiveParquetBatches { kind, reader })
    }

    /// Decode an object without downloading it in full.
    ///
    /// The reader fetches the Parquet footer/metadata and only the column chunks
    /// needed for each record batch through the S3 transfer manager.
    pub async fn decode_reader(
        kind: ArchiveKind,
        reader: ParquetReader,
    ) -> BuildResult<ArchiveBatch> {
        let mut stream = ArchiveParquetStream::new(kind, reader).await?;
        let mut records = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            records.extend(batch.into_records());
        }
        ArchiveBatch::new(kind, records)
    }

    pub(crate) fn writer_properties() -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build()
    }
}
