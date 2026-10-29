use crate::config::ArchiverConfig;
use async_trait::async_trait;
use bytes::Bytes;
use platform_archive::{
    ArchiveBatch, ArchiveBucket, ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchiveParquet,
    ArchivePath, ArchiveRecord, Bucket, IcebergDataFile, ObjectKey, ParquetReader, S3,
};
use platform_errors::BpResult;
use platform_metrics::descriptions::S3_OPERATION_ARCHIVE_UPLOAD;
use platform_metrics::market_data::MarketDataMetrics;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};

const UPLOAD_MAX_ATTEMPTS: u32 = 3;
const UPLOAD_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Writes a batch of de-duplicated records to long-term storage.
#[async_trait]
pub trait ArchiveSink: Send + Sync {
    /// Writes the records, reordering them in place so callers can keep
    /// ownership of the batch and retry it if the write fails.
    async fn write(&self, records: &mut [ArchiveRecord]) -> BpResult<()>;
}

/// Writes Parquet files to S3 and commits them to an Iceberg REST catalog.
///
/// Object names are derived from the Parquet bytes. If an upload succeeds but
/// the catalog commit fails, retrying the same records writes the same object
/// and commits the same data file instead of creating a duplicate. Objects left
/// uncommitted by an earlier run are registered during startup.
pub struct S3ParquetSink {
    transfer_client: aws_sdk_s3_transfer_manager::Client,
    catalog: Arc<dyn ArchiveCatalog>,
    bucket: Bucket,
    prefix: String,
    bucket_millis: i64,
}

impl S3ParquetSink {
    pub async fn connect(
        config: &ArchiverConfig,
        catalog: Arc<dyn ArchiveCatalog>,
    ) -> BpResult<Self> {
        Ok(Self {
            transfer_client: S3::transfer_client().await,
            catalog,
            bucket: Bucket::new(config.s3_bucket.clone()),
            prefix: config.s3_prefix.clone(),
            bucket_millis: config.bucket_millis,
        })
    }

    fn object_key(&self, partition: ArchiveBucket, bytes: &[u8]) -> BpResult<ObjectKey> {
        partition
            .raw_key(
                &self.prefix,
                format!("{}-{:x}.parquet", Self::content_id(bytes), bytes.len()),
            )
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

    fn data_file(&self, key: &ObjectKey, bytes: &[u8], records: usize) -> IcebergDataFile {
        IcebergDataFile::new(
            format!("s3://{}/{key}", self.bucket),
            i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            i64::try_from(records).unwrap_or(i64::MAX),
        )
    }

    pub async fn recover_uncommitted(&self) -> BpResult<()> {
        let prefix = self.prefix.trim_matches('/');
        let list_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let s3_client = S3::s3_client().await;
        let objects = S3::list_objects(
            &s3_client,
            &self.bucket,
            &ObjectKey::new(list_prefix.clone()),
        )
        .await?;

        for kind in [ArchiveKind::Events, ArchiveKind::Candles] {
            let table = ArchiveIcebergTable::raw(kind);
            let registered = self.catalog.registered_data_files(table).await?;
            let mut files = Vec::new();
            for object in &objects {
                let key = object.key.as_str();
                if !key.ends_with(".parquet") {
                    continue;
                }
                let Some(relative) = ArchivePath::relative_key(key, &list_prefix) else {
                    continue;
                };
                let Some((kind_segment, _)) = relative.split_once('/') else {
                    continue;
                };
                if kind_segment != kind.label()
                    || ArchivePath::bucket_start_from_key(relative).is_none()
                {
                    continue;
                }
                let location = format!("s3://{}/{}", self.bucket, key);
                if registered.contains(&location) {
                    continue;
                }
                let Ok(file_size) = u64::try_from(object.size) else {
                    warn!(
                        key,
                        size = object.size,
                        "skipping uncommitted archive object"
                    );
                    continue;
                };
                let reader =
                    ParquetReader::new(&self.transfer_client, &self.bucket, &object.key, file_size);
                let record_count = match reader.row_count().await {
                    Ok(record_count) => record_count,
                    Err(error) => {
                        warn!(%error, key, "skipping unreadable uncommitted archive object");
                        continue;
                    }
                };
                files.push(IcebergDataFile::new(location, object.size, record_count));
            }
            if files.is_empty() {
                continue;
            }
            info!(
                table = table.name(),
                files = files.len(),
                "recovering uncommitted archive files"
            );
            self.catalog.register_files(table, &files, None).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ArchiveSink for S3ParquetSink {
    async fn write(&self, records: &mut [ArchiveRecord]) -> BpResult<()> {
        // Sort by archive bucket in place. Each contiguous group can then be
        // encoded from a borrowed slice instead of cloning every record into a
        // per-partition `Vec`.
        records.sort_by_key(|record| ArchiveBucket::new(record, self.bucket_millis));

        let mut pending: Vec<(ArchiveIcebergTable, Vec<IcebergDataFile>)> = Vec::new();
        let mut start = 0;
        while start < records.len() {
            let partition = ArchiveBucket::new(&records[start], self.bucket_millis);
            let mut end = start + 1;
            while end < records.len()
                && ArchiveBucket::new(&records[end], self.bucket_millis) == partition
            {
                end += 1;
            }

            let batch = ArchiveBatch::new(partition.kind, records[start..end].to_vec())?;
            let bytes = ArchiveParquet::encode(&batch)?;
            let key = self.object_key(partition, &bytes)?;
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
            self.catalog.register_files(table, &files, None).await?;
        }
        Ok(())
    }
}

impl S3ParquetSink {
    fn content_id(bytes: &[u8]) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }
}
