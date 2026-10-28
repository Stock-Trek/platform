use crate::{
    config::CompactorConfig,
    spill::{DedupeSpill, PartitionItem, PartitionRecords, PartitionSpill},
};
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use platform_archive::{
    ArchiveHour, ArchiveKind, ArchiveParquet, ArchiveParquetWriter, ArchivePath, ArchiveRecord,
    SourceBatch,
};
use platform_errors::{BpError, BpResult, DataError};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use tokio::io::{AsyncWriteExt, copy};
use tracing::{info, warn};

const OUTPUT_BATCH_SIZE: usize = 8_192;

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
/// Source objects are streamed to temporary files and decoded in bounded
/// batches. Both the de-duplication map and the partition sort spill to sorted
/// Parquet runs once they reach the configured record limit, so a single large
/// compaction batch does not have to fit in memory.
///
/// TODO: register compacted files in the Iceberg catalog and track/delete the
/// raw source objects after a successful commit.
pub struct Compactor {
    client: S3Client,
    bucket: String,
    output_prefix: String,
    spill_records: usize,
}

impl Compactor {
    pub async fn connect(config: &CompactorConfig) -> BpResult<Self> {
        let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self {
            client: S3Client::new(&sdk_config),
            bucket: config.s3_bucket.clone(),
            output_prefix: config.output_prefix.trim_matches('/').to_string(),
            spill_records: config.spill_records,
        })
    }

    pub async fn compact(&self, batch: &SourceBatch) -> BpResult<CompactionStats> {
        let temp_dir = tempfile::tempdir().map_err(|error| {
            BpError::io(
                "create compaction temporary directory",
                std::env::temp_dir().display().to_string(),
                error,
            )
        })?;
        let mut stats = CompactionStats {
            source_files: batch.files.len(),
            ..CompactionStats::default()
        };

        let mut dedupe = DedupeSpill::new(batch.kind, temp_dir.path(), self.spill_records)?;
        for (index, key) in batch.files.iter().enumerate() {
            let path = temp_dir.path().join(format!("source-{index}.parquet"));
            self.download_to_file(key, &path).await?;

            {
                let file = File::open(&path).map_err(|error| {
                    BpError::io(
                        "open downloaded archive object",
                        path.display().to_string(),
                        error,
                    )
                })?;
                for decoded in ArchiveParquet::decode_batches(batch.kind, file)? {
                    let records = decoded?;
                    stats.records_read += records.len();
                    for record in records {
                        dedupe.push(record)?;
                    }
                }
            }

            if let Err(error) = tokio::fs::remove_file(&path).await {
                warn!(
                    path = %path.display(),
                    %error,
                    "failed to remove temporary source file"
                );
            }
        }

        let mut deduped = dedupe.finish()?;
        let mut partitions = PartitionSpill::new(batch.kind, temp_dir.path(), self.spill_records)?;
        while let Some(record) = deduped.next_record()? {
            partitions.push(record)?;
        }

        let outputs = write_partition_files(batch.kind, partitions.finish()?, temp_dir.path())?;
        stats.records_written = outputs.iter().map(|output| output.records).sum();
        stats.output_files = outputs.len();

        for output in outputs {
            let key = self.output_key(batch, &output.partition)?;
            self.write_object_from_path(&key, &output.path).await?;
            info!(
                partition = %output.partition,
                key = %key,
                records = output.records,
                "wrote compacted partition"
            );
        }

        Ok(stats)
    }

    async fn download_to_file(&self, key: &str, path: &Path) -> BpResult<()> {
        let target = format!("s3://{}/{key}", self.bucket);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| BpError::s3("get raw archive object", target.clone(), error))?;

        let mut body = Box::pin(output.body.into_async_read());
        let mut file = tokio::fs::File::create(path).await.map_err(|error| {
            BpError::io(
                "create temporary source file",
                path.display().to_string(),
                error,
            )
        })?;
        copy(&mut body, &mut file)
            .await
            .map_err(|error| BpError::io("stream raw archive object", target, error))?;
        file.flush().await.map_err(|error| {
            BpError::io(
                "flush temporary source file",
                path.display().to_string(),
                error,
            )
        })?;
        Ok(())
    }

    async fn write_object_from_path(&self, key: &str, path: &Path) -> BpResult<()> {
        let body = ByteStream::from_path(path).await.map_err(|error| {
            BpError::io(
                "read compacted parquet file",
                path.display().to_string(),
                error,
            )
        })?;
        let target = format!("s3://{}/{key}", self.bucket);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/vnd.apache.parquet")
            .body(body)
            .send()
            .await
            .map_err(|error| BpError::s3("put compacted parquet object", target, error))?;
        Ok(())
    }

    fn output_key(&self, batch: &SourceBatch, partition: &str) -> BpResult<String> {
        let hour = ArchiveHour::from_timestamp(batch.bucket_start).ok_or(BpError::Data(
            DataError::DateTimeOutOfRange {
                operation: "build compacted object key",
            },
        ))?;
        Ok(ArchivePath::join(
            &self.output_prefix,
            &format!("{partition}/{}", hour.file_path("data.parquet")),
        ))
    }
}

struct PartitionOutput {
    partition: String,
    path: PathBuf,
    records: usize,
}

fn write_partition_files(
    kind: ArchiveKind,
    mut records: PartitionRecords,
    directory: &Path,
) -> BpResult<Vec<PartitionOutput>> {
    let mut outputs = Vec::new();
    let mut current: Option<(PartitionOutput, ArchiveParquetWriter<File>)> = None;
    let mut buffer = Vec::<ArchiveRecord>::with_capacity(OUTPUT_BATCH_SIZE);

    while let Some(PartitionItem {
        partition, record, ..
    }) = records.next_item()?
    {
        let start_new_partition = current
            .as_ref()
            .is_none_or(|(output, _)| output.partition != partition);
        if start_new_partition {
            if let Some((output, mut writer)) = current.take() {
                if !buffer.is_empty() {
                    writer.write(&buffer)?;
                    buffer.clear();
                }
                writer.close()?;
                outputs.push(output);
            }

            let path = directory.join(format!("compacted-{}.parquet", outputs.len()));
            let file = File::create(&path).map_err(|error| {
                BpError::io(
                    "create compacted partition file",
                    path.display().to_string(),
                    error,
                )
            })?;
            current = Some((
                PartitionOutput {
                    partition,
                    path,
                    records: 0,
                },
                ArchiveParquetWriter::try_new(kind, file)?,
            ));
        }

        buffer.push(record);
        if let Some((output, writer)) = current.as_mut() {
            output.records += 1;
            if buffer.len() >= OUTPUT_BATCH_SIZE {
                writer.write(&buffer)?;
                buffer.clear();
            }
        }
    }

    if let Some((output, mut writer)) = current.take() {
        if !buffer.is_empty() {
            writer.write(&buffer)?;
        }
        writer.close()?;
        outputs.push(output);
    }

    Ok(outputs)
}
