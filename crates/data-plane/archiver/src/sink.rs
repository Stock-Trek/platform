use crate::config::ArchiverConfig;
use async_trait::async_trait;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use platform_archive::{ArchiveBucket, ArchiveParquet, ArchiveRecord};
use platform_errors::{BpError, BpResult};
use std::collections::HashMap;
use uuid::Uuid;

/// Writes a batch of de-duplicated records to long-term storage.
#[async_trait]
pub trait ArchiveSink: Send + Sync {
    async fn write(&self, records: &[ArchiveRecord]) -> BpResult<()>;
}

/// Writes Parquet files to S3, partitioned by record kind and exchange time.
///
/// TODO: register the files in an Apache Iceberg table/catalog instead of only
/// writing raw Parquet objects, and use Iceberg metadata for retries and
/// compaction.
pub struct S3ParquetSink {
    client: S3Client,
    bucket: String,
    prefix: String,
    bucket_millis: i64,
}

impl S3ParquetSink {
    pub async fn connect(config: &ArchiverConfig) -> BpResult<Self> {
        let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self {
            client: S3Client::new(&sdk_config),
            bucket: config.s3_bucket.clone(),
            prefix: config.s3_prefix.clone(),
            bucket_millis: config.bucket_millis,
        })
    }

    fn object_key(&self, partition: ArchiveBucket) -> BpResult<String> {
        partition.raw_key(&self.prefix, format!("{}.parquet", Uuid::new_v4()))
    }
}

#[async_trait]
impl ArchiveSink for S3ParquetSink {
    async fn write(&self, records: &[ArchiveRecord]) -> BpResult<()> {
        let mut partitions: HashMap<ArchiveBucket, Vec<ArchiveRecord>> = HashMap::new();
        for record in records {
            partitions
                .entry(ArchiveBucket::new(record, self.bucket_millis))
                .or_default()
                .push(record.clone());
        }

        for (partition, records) in partitions {
            let key = self.object_key(partition)?;
            let bytes = ArchiveParquet::encode(partition.kind, &records)?;
            let target = format!("s3://{}/{key}", self.bucket);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .content_type("application/vnd.apache.parquet")
                .body(ByteStream::from(bytes))
                .send()
                .await
                .map_err(|error| BpError::s3("put archived parquet object", target, error))?;
        }
        Ok(())
    }
}
