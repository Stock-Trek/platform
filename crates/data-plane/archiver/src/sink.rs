use crate::config::ArchiverConfig;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use platform_archive::{
    ArchiveBatch, ArchiveBucket, ArchiveCatalog, ArchiveIcebergTable, ArchiveParquet,
    ArchiveRecord, Bucket, ContentHash, IcebergDataFile, ObjectKey, ObjectStore, ParquetRowCounter,
    RoutingBucket, S3, S3ObjectStore, S3ParquetRowCounter,
};
use platform_errors::PlatformResult;
use platform_lease::LeaseStore;
use platform_metrics::MarketDataMetrics;
use platform_metrics::S3_OPERATION_ARCHIVE_UPLOAD;
use platform_time::RetryPolicy;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::warn;

const UPLOAD_MAX_ATTEMPTS: usize = 3;
const UPLOAD_RETRY_BACKOFF: Duration = Duration::from_millis(500);
const UPLOAD_MAX_BACKOFF: Duration = Duration::from_millis(500 * UPLOAD_MAX_ATTEMPTS as u64);

/// Writes a batch of de-duplicated records to long-term storage.
#[async_trait]
pub trait ArchiveSink: Send + Sync {
    /// Writes the records, reordering them in place so callers can keep
    /// ownership of the batch and retry it if the write fails.
    async fn write(&self, records: &mut [ArchiveRecord]) -> PlatformResult<()>;
}

/// Writes Parquet files to S3 and commits them to an Iceberg REST catalog.
///
/// Object names are derived from the Parquet bytes. If an upload succeeds but
/// the catalog commit fails, retrying the same records writes the same object
/// and commits the same data file instead of creating a duplicate. Objects left
/// uncommitted by an earlier run are registered during startup.
pub struct S3ParquetSink {
    pub(crate) s3_client: S3Client,
    pub(crate) objects: Arc<dyn ObjectStore>,
    pub(crate) row_counter: Arc<dyn ParquetRowCounter>,
    pub(crate) catalog: Arc<dyn ArchiveCatalog>,
    pub(crate) leases: Arc<dyn LeaseStore>,
    pub(crate) bucket: Bucket,
    pub(crate) prefix: String,
    pub(crate) bucket_millis: i64,
    pub(crate) lease_ttl: Duration,
    pub(crate) max_work: Duration,
    pub(crate) lease_initial_token: u64,
    pub(crate) lease_clock_skew: Duration,
    pub(crate) recovery_page_files: usize,
    pub(crate) recovery_page_bytes: usize,
}

impl S3ParquetSink {
    pub async fn connect(
        config: &ArchiverConfig,
        catalog: Arc<dyn ArchiveCatalog>,
        leases: Arc<dyn LeaseStore>,
    ) -> PlatformResult<Self> {
        let s3_client = S3::s3_client().await;
        Ok(Self {
            objects: Arc::new(S3ObjectStore::new(s3_client.clone())),
            row_counter: Arc::new(S3ParquetRowCounter::new(s3_client.clone())),
            s3_client,
            catalog,
            leases,
            bucket: Bucket::new(config.s3_bucket.clone()),
            prefix: config.s3_prefix.clone(),
            bucket_millis: config.bucket_millis,
            lease_ttl: config.lease_ttl,
            max_work: config.max_work,
            lease_initial_token: config.lease_initial_token,
            lease_clock_skew: config.lease_clock_skew,
            recovery_page_files: config.recovery_page_files,
            recovery_page_bytes: config.recovery_page_bytes,
        })
    }

    fn object_key(
        &self,
        partition: ArchiveBucket,
        routing_bucket: RoutingBucket,
        bytes: &[u8],
    ) -> PlatformResult<ObjectKey> {
        partition
            .raw_key(
                &self.prefix,
                routing_bucket,
                format!(
                    "{}-{:x}.parquet",
                    ContentHash::from_bytes(bytes),
                    bytes.len()
                ),
            )
            .map(ObjectKey::new)
    }

    async fn upload_with_retry(&self, key: &ObjectKey, bytes: Bytes) -> PlatformResult<()> {
        let retry = RetryPolicy::linear(
            UPLOAD_MAX_ATTEMPTS,
            UPLOAD_RETRY_BACKOFF,
            UPLOAD_MAX_BACKOFF,
        )
        .without_jitter();
        let mut attempt = 1_usize;
        loop {
            match S3::upload_bytes(&self.s3_client, &self.bucket, key, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if !retry.should_retry(attempt) {
                        return Err(error);
                    }
                    MarketDataMetrics::record_s3_retry(S3_OPERATION_ARCHIVE_UPLOAD);
                    warn!(%error, attempt, "S3 upload failed, retrying");
                    sleep(retry.delay(attempt - 1)).await;
                    attempt += 1;
                }
            }
        }
    }

    fn data_file(&self, key: &ObjectKey, bytes: &[u8], records: usize) -> IcebergDataFile {
        IcebergDataFile::new(
            format!("s3://{}/{key}", self.bucket),
            i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            i64::try_from(records).unwrap_or(i64::MAX),
        )
    }
}

#[async_trait]
impl ArchiveSink for S3ParquetSink {
    async fn write(&self, records: &mut [ArchiveRecord]) -> PlatformResult<()> {
        // Sort by archive bucket in place. Each contiguous group can then be
        // encoded from a borrowed slice instead of cloning every record into a
        // per-partition `Vec`.
        records.sort_by_key(|record| {
            (
                ArchiveBucket::new(record, self.bucket_millis),
                record.routing_bucket(),
            )
        });

        let mut pending: Vec<(ArchiveIcebergTable, Vec<IcebergDataFile>)> = Vec::new();
        let mut start = 0;
        while start < records.len() {
            let partition = ArchiveBucket::new(&records[start], self.bucket_millis);
            let routing_bucket = records[start].routing_bucket();
            let mut end = start + 1;
            while end < records.len()
                && ArchiveBucket::new(&records[end], self.bucket_millis) == partition
                && records[end].routing_bucket() == routing_bucket
            {
                end += 1;
            }

            let batch = ArchiveBatch::new(partition.kind, records[start..end].to_vec())?;
            let bytes = ArchiveParquet::encode(&batch)?;
            let key = self.object_key(partition, routing_bucket, &bytes)?;
            self.upload_with_retry(&key, Bytes::from(bytes.clone()))
                .await?;

            let table = ArchiveIcebergTable::raw(partition.kind);
            let data_file = self.data_file(&key, &bytes, end - start);
            match pending.iter_mut().find(|(pending, _)| *pending == table) {
                Some((_, files)) => files.push(data_file),
                None => pending.push((table, vec![data_file])),
            }
            start = end;
        }

        for (table, files) in pending {
            self.register_flush(table, files).await?;
        }
        Ok(())
    }
}
