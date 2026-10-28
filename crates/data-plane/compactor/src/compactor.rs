use crate::config::CompactorConfig;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use bytes::Bytes;
use platform_archive::{
    ArchiveParquet, ArchivePartition, ArchivePath, ArchiveRecord, ArchiveRecordKey, SourceBatch,
};
use platform_errors::{BpError, BpResult};
use std::collections::HashMap;
use tracing::info;

#[derive(Debug, Default, Clone, Copy)]
pub struct CompactionStats {
    pub source_files: usize,
    pub records_read: usize,
    pub records_written: usize,
    pub output_files: usize,
}

/// Reads raw archive files, de-duplicates and sorts their rows, then writes one
/// compacted file per symbol partition.
///
/// TODO: register compacted files in the Iceberg catalog and track/delete the
/// raw source objects after a successful commit.
pub struct Compactor {
    client: S3Client,
    bucket: String,
    output_prefix: String,
}

impl Compactor {
    pub async fn connect(config: &CompactorConfig) -> BpResult<Self> {
        let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self {
            client: S3Client::new(&sdk_config),
            bucket: config.s3_bucket.clone(),
            output_prefix: config.output_prefix.trim_matches('/').to_string(),
        })
    }

    pub async fn compact(&self, batch: &SourceBatch) -> BpResult<CompactionStats> {
        let mut records = HashMap::<ArchiveRecordKey, ArchiveRecord>::new();
        let mut stats = CompactionStats {
            source_files: batch.files.len(),
            ..CompactionStats::default()
        };

        for key in &batch.files {
            let bytes = self.read_object(key).await?;
            let decoded = ArchiveParquet::decode(batch.kind, bytes)?;
            stats.records_read += decoded.len();
            for record in decoded {
                records.insert(record.key(), record);
            }
        }

        let mut partitions = HashMap::<ArchivePartition, Vec<ArchiveRecord>>::new();
        for record in records.into_values() {
            partitions
                .entry(record.partition())
                .or_default()
                .push(record);
        }

        for (partition, mut partition_records) in partitions {
            partition_records.sort_by_key(ArchiveRecord::sort_key);
            stats.records_written += partition_records.len();
            let bytes = ArchiveParquet::encode(batch.kind, &partition_records)?;
            let key = self.output_key(batch, &partition)?;
            self.write_object(&key, bytes).await?;
            stats.output_files += 1;
            info!(
                partition = %partition.path(),
                key = %key,
                records = partition_records.len(),
                "wrote compacted partition"
            );
        }

        Ok(stats)
    }

    async fn read_object(&self, key: &str) -> BpResult<Bytes> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                BpError::s3(
                    "get raw archive object",
                    format!("s3://{}/{key}", self.bucket),
                    error,
                )
            })?;
        output
            .body
            .collect()
            .await
            .map(|body| body.into_bytes())
            .map_err(|error| {
                BpError::s3(
                    "read raw archive object body",
                    format!("s3://{}/{key}", self.bucket),
                    error,
                )
            })
    }

    async fn write_object(&self, key: &str, bytes: Vec<u8>) -> BpResult<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/vnd.apache.parquet")
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|error| {
                BpError::s3(
                    "put compacted parquet object",
                    format!("s3://{}/{key}", self.bucket),
                    error,
                )
            })?;
        Ok(())
    }

    fn output_key(&self, batch: &SourceBatch, partition: &ArchivePartition) -> BpResult<String> {
        ArchivePath::compacted_key(
            &self.output_prefix,
            partition,
            batch.bucket_start,
            "data.parquet",
        )
    }
}
