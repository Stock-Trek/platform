use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use platform_archive::{
    ArchiveBatch, ArchiveBucket, ArchiveCatalog, ArchiveIcebergTable, ArchiveKind,
    ArchiveParquetWriter, ArchivePartition, ArchiveRecord, ArchiveTableLayer, Bucket,
    IcebergDataFile, ObjectKey, RoutingBucket, S3,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_market_data::ClosedCandle;
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::info;

const OUTPUT_BATCH_SIZE: usize = 8_192;
const CONTENT_ID_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const CONTENT_ID_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Counts the archive objects produced by a rebuild.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CandleWriteStats {
    pub objects: usize,
    pub records: usize,
}

/// Supplies closed candles to the sink in its object order: archive bucket,
/// routing bucket, compacted partition, sort key, and record identity.
pub trait CandleRecordSource: Send {
    fn next_candle(&mut self) -> PlatformResult<Option<ClosedCandle>>;
}

/// Writes regenerated candles to the archive.
#[async_trait]
pub trait CandleSink: Send + Sync {
    /// Writes every candle from the ordered source, one object per partition
    /// group, and registers all objects after they are uploaded.
    async fn write(
        &self,
        candles: &mut dyn CandleRecordSource,
        work_dir: &Path,
    ) -> PlatformResult<CandleWriteStats>;
}

/// Writes regenerated candles to the selected archive layer.
///
/// Raw output is grouped into hourly objects and registered in the raw candle
/// Iceberg table. Compacted output is additionally partitioned by candle key
/// and registered in the compacted candle table. Every object is encoded to a
/// temporary file and uploaded from that file, so an output object never has
/// to be buffered in memory.
pub struct S3CandleSink {
    s3_client: aws_sdk_s3::Client,
    catalog: Arc<dyn ArchiveCatalog>,
    bucket: Bucket,
    prefix: String,
    bucket_millis: i64,
    layer: ArchiveTableLayer,
}

impl S3CandleSink {
    pub async fn connect(
        bucket: &str,
        prefix: &str,
        bucket_millis: i64,
        catalog: Arc<dyn ArchiveCatalog>,
        layer: ArchiveTableLayer,
    ) -> PlatformResult<Self> {
        Ok(Self {
            s3_client: S3::s3_client().await,
            catalog,
            bucket: Bucket::new(bucket),
            prefix: prefix.trim_matches('/').to_string(),
            bucket_millis,
            layer,
        })
    }

    fn object_key(
        &self,
        bucket: ArchiveBucket,
        partition: &ArchivePartition,
        routing_bucket: RoutingBucket,
        content_id: u64,
        length: u64,
    ) -> PlatformResult<ObjectKey> {
        let file_name = format!("{content_id}-{length:x}.parquet");
        let key = match self.layer {
            ArchiveTableLayer::Raw => bucket.raw_key(&self.prefix, routing_bucket, file_name),
            ArchiveTableLayer::Compacted => {
                bucket.compacted_key(&self.prefix, partition, routing_bucket, file_name)
            }
        }?;
        Ok(ObjectKey::new(key))
    }

    fn data_file(&self, key: &ObjectKey, length: u64, records: usize) -> IcebergDataFile {
        IcebergDataFile::new(
            format!("s3://{}/{key}", self.bucket),
            i64::try_from(length).unwrap_or(i64::MAX),
            i64::try_from(records).unwrap_or(i64::MAX),
        )
    }

    async fn finish_partition(
        &self,
        mut file: PartitionFile,
        buffer: &mut ArchiveBatch,
        data_files: &mut Vec<IcebergDataFile>,
        stats: &mut CandleWriteStats,
    ) -> PlatformResult<()> {
        if !buffer.is_empty() {
            file.writer.write(buffer)?;
            file.records += buffer.len();
            buffer.clear();
        }
        file.writer.close()?;
        let (content_id, length) = Self::hash_file(&file.path)?;
        let key = self.object_key(
            file.bucket,
            &file.partition,
            file.routing_bucket,
            content_id,
            length,
        )?;
        self.upload_partition(&key, &file.path).await?;
        data_files.push(self.data_file(&key, length, file.records));
        stats.objects += 1;
        stats.records += file.records;
        info!(
            key = %key,
            records = file.records,
            "wrote rebuilt candle object"
        );
        Ok(())
    }

    fn hash_file(path: &Path) -> PlatformResult<(u64, u64)> {
        let mut file = File::open(path).map_err(|error| {
            PlatformError::io(
                "read rebuilt candle file",
                path.display().to_string(),
                error,
            )
        })?;
        let mut hash = CONTENT_ID_OFFSET_BASIS;
        let mut length = 0_u64;
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut chunk).map_err(|error| {
                PlatformError::io(
                    "read rebuilt candle file",
                    path.display().to_string(),
                    error,
                )
            })?;
            if read == 0 {
                break;
            }
            for byte in &chunk[..read] {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(CONTENT_ID_PRIME);
            }
            length += u64::try_from(read).unwrap_or(0);
        }
        Ok((hash, length))
    }

    async fn upload_partition(&self, key: &ObjectKey, path: &Path) -> PlatformResult<()> {
        let target = format!("s3://{}/{}", self.bucket, key);
        let body = ByteStream::from_path(path).await.map_err(|error| {
            PlatformError::io(
                "read rebuilt candle file",
                path.display().to_string(),
                error,
            )
        })?;
        self.s3_client
            .put_object()
            .bucket(self.bucket.as_str())
            .key(key.as_str())
            .content_type("application/vnd.apache.parquet")
            .body(body)
            .send()
            .await
            .map_err(|error| PlatformError::s3("put rebuilt candle object", target, error))?;
        Ok(())
    }
}

struct PartitionFile {
    bucket: ArchiveBucket,
    routing_bucket: RoutingBucket,
    partition: ArchivePartition,
    path: PathBuf,
    writer: ArchiveParquetWriter<File>,
    records: usize,
}

#[async_trait]
impl CandleSink for S3CandleSink {
    async fn write(
        &self,
        candles: &mut dyn CandleRecordSource,
        work_dir: &Path,
    ) -> PlatformResult<CandleWriteStats> {
        let mut stats = CandleWriteStats::default();
        let mut data_files = Vec::new();
        let mut buffer = ArchiveBatch::new(ArchiveKind::Candles, Vec::new())?;
        let mut current: Option<PartitionFile> = None;

        while let Some(candle) = candles.next_candle()? {
            let record = ArchiveRecord::ClosedCandle(candle);
            let bucket = ArchiveBucket::new(&record, self.bucket_millis);
            let routing_bucket = record.routing_bucket();
            let partition = record.partition();
            let continues = current.as_ref().is_some_and(|file| {
                file.bucket == bucket
                    && file.routing_bucket == routing_bucket
                    && (self.layer == ArchiveTableLayer::Raw || file.partition == partition)
            });
            if !continues {
                if let Some(file) = current.take() {
                    self.finish_partition(file, &mut buffer, &mut data_files, &mut stats)
                        .await?;
                }
                current = Some(PartitionFile::create(
                    work_dir,
                    stats.objects,
                    bucket,
                    routing_bucket,
                    partition,
                )?);
            }
            let file = current
                .as_mut()
                .expect("partition file created before writing");
            buffer.push(record)?;
            if buffer.len() >= OUTPUT_BATCH_SIZE {
                file.writer.write(&buffer)?;
                file.records += buffer.len();
                buffer.clear();
            }
        }

        if let Some(file) = current.take() {
            self.finish_partition(file, &mut buffer, &mut data_files, &mut stats)
                .await?;
        }

        if !data_files.is_empty() {
            self.catalog
                .register_files(
                    ArchiveIcebergTable::new(self.layer, ArchiveKind::Candles),
                    &data_files,
                    None,
                )
                .await?;
        }
        info!(
            layer = self.layer.label(),
            objects = stats.objects,
            records = stats.records,
            "wrote rebuilt candles to the archive"
        );
        Ok(stats)
    }
}

impl PartitionFile {
    fn create(
        work_dir: &Path,
        index: usize,
        bucket: ArchiveBucket,
        routing_bucket: RoutingBucket,
        partition: ArchivePartition,
    ) -> PlatformResult<Self> {
        let path = work_dir.join(format!("candles-{index}.parquet"));
        let file = File::create(&path).map_err(|error| {
            PlatformError::io(
                "create rebuilt candle file",
                path.display().to_string(),
                error,
            )
        })?;
        let writer = ArchiveParquetWriter::try_new(ArchiveKind::Candles, file)?;
        Ok(Self {
            bucket,
            routing_bucket,
            partition,
            path,
            writer,
            records: 0,
        })
    }
}
