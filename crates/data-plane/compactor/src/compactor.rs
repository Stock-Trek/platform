use crate::{
    config::CompactorConfig,
    partition_output::PartitionOutput,
    spill::{DedupeSpill, PartitionSpill},
};
use async_trait::async_trait;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use platform_archive::{
    ArchiveCatalog, ArchiveHour, ArchiveIcebergTable, ArchiveParquet, ArchivePath,
    ArchiveRegistration, ContentHash, ContentHasher, IcebergDataFile, RegisterOutcome, S3,
    SourceBatch,
};
use platform_errors::{DataError, PlatformError, PlatformResult};
use platform_lease::{Lease, LeaseGuard, LeaseRequest, LeaseScope, LeaseStore};
use std::{fs::File, path::Path, sync::Arc};
use tokio::io::{AsyncWriteExt, copy};
use tracing::{info, warn};

#[derive(Debug, Default, Clone, Copy)]
pub struct CompactionStats {
    pub source_files: usize,
    pub records_read: usize,
    pub records_written: usize,
    pub output_files: usize,
    pub skipped: bool,
}

/// Compacts a batch of raw archive files into partitioned output files.
#[async_trait]
pub trait BatchCompactor: Send + Sync {
    async fn compact(&self, batch: &SourceBatch) -> PlatformResult<CompactionStats>;
}

/// Reads raw archive files, de-duplicates and sorts their rows, then writes one
/// compacted file per symbol partition.
///
/// Source objects are streamed to temporary files and decoded in bounded
/// batches. Both the de-duplication map and the partition sort spill to sorted
/// Parquet runs once they reach the configured record limit, so a single large
/// compaction batch does not have to fit in memory.
///
/// Compacted files and the raw source object URIs are committed in the same
/// Iceberg snapshot. That snapshot is the compacted/raw handoff: source objects
/// below it are never replayed, while all other raw objects remain in the tail.
/// Raw objects are intentionally left in place.
pub struct Compactor {
    client: S3Client,
    catalog: Arc<dyn ArchiveCatalog>,
    bucket: String,
    output_prefix: String,
    spill_records: usize,
    leases: Arc<dyn LeaseStore>,
    lease_ttl_events: std::time::Duration,
    lease_ttl_candles: std::time::Duration,
    max_work: std::time::Duration,
    lease_initial_token: u64,
}

impl Compactor {
    pub async fn connect(
        config: &CompactorConfig,
        catalog: Arc<dyn ArchiveCatalog>,
        leases: Arc<dyn LeaseStore>,
    ) -> PlatformResult<Self> {
        Ok(Self {
            client: S3::s3_client().await,
            catalog,
            bucket: config.s3_bucket.clone(),
            output_prefix: config.output_prefix.trim_matches('/').to_string(),
            spill_records: config.spill_records,
            leases,
            lease_ttl_events: config.lease_ttl_for(platform_archive::ArchiveKind::Events),
            lease_ttl_candles: config.lease_ttl_for(platform_archive::ArchiveKind::Candles),
            max_work: config.max_work,
            lease_initial_token: config.lease_initial_token,
        })
    }

    pub async fn compact(&self, batch: &SourceBatch) -> PlatformResult<CompactionStats> {
        let scope = LeaseScope::new(Self::lease_scope(batch));
        let initial_token = i64::try_from(self.lease_initial_token).unwrap_or(i64::MAX);
        let lease_ttl = match batch.kind {
            platform_archive::ArchiveKind::Events => self.lease_ttl_events,
            platform_archive::ArchiveKind::Candles => self.lease_ttl_candles,
        };
        let request = LeaseRequest::new(scope, Lease::new_owner_id(), lease_ttl)
            .with_initial_token(initial_token)?;
        let Some(mut guard) =
            LeaseGuard::acquire(self.leases.clone(), request, self.max_work).await?
        else {
            info!(
                prefix = batch.prefix,
                "compaction skipped because another compactor holds the lease"
            );
            return Ok(CompactionStats {
                source_files: batch.files.len(),
                skipped: true,
                ..CompactionStats::default()
            });
        };
        let processed = self
            .catalog
            .processed_source_files(ArchiveIcebergTable::compacted(batch.kind))
            .await?;
        let claimed = batch
            .files
            .iter()
            .filter(|file| processed.contains(&format!("s3://{}/{file}", self.bucket)))
            .cloned()
            .collect::<Vec<_>>();
        if !claimed.is_empty() {
            warn!(
                kind = batch.kind.label(),
                prefix = %batch.prefix,
                claimed_sources = ?claimed,
                "compaction skipped because the source set is already claimed"
            );
            info!(
                prefix = batch.prefix,
                "compaction skipped because the source set is already claimed"
            );
            guard.release().await?;
            return Ok(CompactionStats {
                source_files: batch.files.len(),
                skipped: true,
                ..CompactionStats::default()
            });
        }
        let result = self.compact_leased(batch, &mut guard).await;
        if let Err(error) = guard.release().await {
            warn!(%error, prefix = batch.prefix, "failed to release compaction lease");
        }
        result
    }

    async fn compact_leased(
        &self,
        batch: &SourceBatch,
        guard: &mut LeaseGuard,
    ) -> PlatformResult<CompactionStats> {
        let temp_dir = tempfile::tempdir().map_err(|error| {
            PlatformError::io(
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
                    PlatformError::io(
                        "open downloaded archive object",
                        path.display().to_string(),
                        error,
                    )
                })?;
                for decoded in ArchiveParquet::decode_batches(batch.kind, file)? {
                    let records = decoded?.into_records();
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

        let outputs =
            PartitionOutput::write_all(batch.kind, partitions.finish()?, temp_dir.path())?;
        stats.records_written = outputs.iter().map(|output| output.records).sum();
        stats.output_files = outputs.len();

        let table = ArchiveIcebergTable::compacted(batch.kind);
        let mut data_files = Vec::with_capacity(outputs.len());
        for output in outputs {
            let key = self.output_key(
                batch,
                &output.partition,
                &batch.digest(),
                output.content_hash,
                output.length,
            )?;
            guard.ensure_writable().await?;
            self.write_object_from_path(&key, &output.path).await?;
            data_files.push(IcebergDataFile::new(
                format!("s3://{}/{key}", self.bucket),
                i64::try_from(output.length).unwrap_or(i64::MAX),
                i64::try_from(output.records).unwrap_or(i64::MAX),
            ));
            info!(
                partition = %output.partition,
                key = %key,
                records = output.records,
                "wrote compacted partition"
            );
        }

        let source_files = batch
            .files
            .iter()
            .map(|key| format!("s3://{}/{key}", self.bucket))
            .collect::<Vec<_>>();
        guard.ensure_writable().await?;
        let registration = ArchiveRegistration::new(data_files, Some(source_files))?;
        match self.catalog.register_files(table, &registration).await? {
            RegisterOutcome::Applied | RegisterOutcome::AlreadyApplied => {}
            RegisterOutcome::Conflict(conflict) => {
                warn!(
                    kind = batch.kind.label(),
                    prefix = %batch.prefix,
                    claimed_sources = ?conflict.claimed_sources,
                    live_outputs = ?conflict.live_outputs,
                    "compaction registration conflicted"
                );
                stats.skipped = true;
            }
        }

        Ok(stats)
    }

    fn lease_scope(batch: &SourceBatch) -> String {
        let mut hasher = ContentHasher::new();
        hasher.update(batch.prefix.as_bytes());
        format!(
            "compact:{}:{}:{}",
            batch.kind.label(),
            hasher.finish(),
            batch.bucket_start.0
        )
    }

    async fn download_to_file(&self, key: &str, path: &Path) -> PlatformResult<()> {
        let target = format!("s3://{}/{key}", self.bucket);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| PlatformError::s3("get raw archive object", target.clone(), error))?;

        let mut body = Box::pin(output.body.into_async_read());
        let mut file = tokio::fs::File::create(path).await.map_err(|error| {
            PlatformError::io(
                "create temporary source file",
                path.display().to_string(),
                error,
            )
        })?;
        copy(&mut body, &mut file)
            .await
            .map_err(|error| PlatformError::io("stream raw archive object", target, error))?;
        file.flush().await.map_err(|error| {
            PlatformError::io(
                "flush temporary source file",
                path.display().to_string(),
                error,
            )
        })?;
        Ok(())
    }

    async fn write_object_from_path(&self, key: &str, path: &Path) -> PlatformResult<()> {
        let body = ByteStream::from_path(path).await.map_err(|error| {
            PlatformError::io(
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
            .map_err(|error| PlatformError::s3("put compacted parquet object", target, error))?;
        Ok(())
    }

    fn output_key(
        &self,
        batch: &SourceBatch,
        partition: &str,
        source_digest: &str,
        content_hash: ContentHash,
        length: u64,
    ) -> PlatformResult<String> {
        let hour = ArchiveHour::from_timestamp(batch.bucket_start).ok_or(PlatformError::Data(
            DataError::DateTimeOutOfRange {
                operation: "build compacted object key",
            },
        ))?;
        Ok(ArchivePath::join(
            &self.output_prefix,
            &format!(
                "{partition}/{}",
                hour.file_path(format!("{source_digest}-{content_hash}-{length:x}.parquet"))
            ),
        ))
    }
}

#[async_trait]
impl BatchCompactor for Compactor {
    async fn compact(&self, batch: &SourceBatch) -> PlatformResult<CompactionStats> {
        Compactor::compact(self, batch).await
    }
}
