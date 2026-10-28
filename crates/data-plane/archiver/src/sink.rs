use crate::config::ArchiverConfig;
use async_trait::async_trait;
use bytes::Bytes;
use platform_archive::{
    ArchiveBatch, ArchiveBucket, ArchiveParquet, ArchiveRecord, Bucket, ObjectKey, S3,
};
use platform_errors::BpResult;
use platform_metrics::descriptions::S3_OPERATION_ARCHIVE_UPLOAD;
use platform_metrics::market_data::MarketDataMetrics;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

const UPLOAD_MAX_ATTEMPTS: u32 = 3;
const UPLOAD_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Writes a batch of de-duplicated records to long-term storage.
#[async_trait]
pub trait ArchiveSink: Send + Sync {
    /// Writes the records, reordering them in place so callers can keep
    /// ownership of the batch and retry it if the write fails.
    async fn write(&self, records: &mut [ArchiveRecord]) -> BpResult<()>;
}

/// Writes Parquet files to S3, partitioned by record kind and exchange time.
///
/// TODO: register the files in an Apache Iceberg table/catalog instead of only
/// writing raw Parquet objects, and use Iceberg metadata for retries and
/// compaction.
pub struct S3ParquetSink {
    transfer_client: aws_sdk_s3_transfer_manager::Client,
    bucket: Bucket,
    prefix: String,
    bucket_millis: i64,
}

impl S3ParquetSink {
    pub async fn connect(config: &ArchiverConfig) -> BpResult<Self> {
        Ok(Self {
            transfer_client: S3::transfer_client().await,
            bucket: Bucket::new(config.s3_bucket.clone()),
            prefix: config.s3_prefix.clone(),
            bucket_millis: config.bucket_millis,
        })
    }

    fn object_key(&self, partition: ArchiveBucket) -> BpResult<ObjectKey> {
        partition
            .raw_key(&self.prefix, format!("{}.parquet", Uuid::new_v4()))
            .map(ObjectKey::new)
    }

    async fn upload_with_retry(&self, key: &ObjectKey, bytes: Bytes) -> BpResult<()> {
        let mut attempt = 1_u32;
        loop {
            match S3::upload_bytes(&self.transfer_client, &self.bucket, key, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if attempt >= UPLOAD_MAX_ATTEMPTS {
                        return Err(error);
                    }
                    MarketDataMetrics::record_s3_retry(S3_OPERATION_ARCHIVE_UPLOAD);
                    warn!(%error, attempt, "S3 upload failed, retrying");
                    sleep(UPLOAD_RETRY_BACKOFF * attempt).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait]
impl ArchiveSink for S3ParquetSink {
    async fn write(&self, records: &mut [ArchiveRecord]) -> BpResult<()> {
        // Sort by archive bucket in place. Each contiguous group can then be
        // encoded from a borrowed slice instead of cloning every record into a
        // per-partition `Vec`.
        records.sort_by_key(|record| ArchiveBucket::new(record, self.bucket_millis));

        let mut start = 0;
        while start < records.len() {
            let partition = ArchiveBucket::new(&records[start], self.bucket_millis);
            let mut end = start + 1;
            while end < records.len()
                && ArchiveBucket::new(&records[end], self.bucket_millis) == partition
            {
                end += 1;
            }

            let key = self.object_key(partition)?;
            let batch = ArchiveBatch::new(partition.kind, records[start..end].to_vec())?;
            let bytes = ArchiveParquet::encode(&batch)?;
            self.upload_with_retry(&key, Bytes::from(bytes)).await?;
            start = end;
        }
        Ok(())
    }
}
