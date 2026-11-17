mod storage;

#[cfg(test)]
mod compactor_tests;

use crate::{
    config::CompactorConfig,
    partition_output::PartitionOutput,
    spill::{DedupeSpill, PartitionSpill},
};
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveParquet, ArchiveRegistration, CommitContext,
    CompactionId, IcebergDataFile, ObjectStore, RegisterOutcome, S3, S3ObjectStore, SourceBatch,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::{Lease, LeaseGuard, LeaseRequest, LeaseScope, LeaseStore};
use platform_metrics::MarketDataMetrics;
use std::{fs::File, sync::Arc};
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

    async fn maintain(&self) -> PlatformResult<()> {
        Ok(())
    }
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
    objects: Arc<dyn ObjectStore>,
    catalog: Arc<dyn ArchiveCatalog>,
    bucket: String,
    output_prefix: String,
    spill_records: usize,
    leases: Arc<dyn LeaseStore>,
    lease_ttl_events: std::time::Duration,
    lease_ttl_candles: std::time::Duration,
    max_work: std::time::Duration,
    maintenance_safety: std::time::Duration,
    lease_initial_token: u64,
    lease_clock_skew: std::time::Duration,
}

impl Compactor {
    pub async fn connect(
        config: &CompactorConfig,
        catalog: Arc<dyn ArchiveCatalog>,
        leases: Arc<dyn LeaseStore>,
    ) -> PlatformResult<Self> {
        let client = S3::s3_client().await;
        Ok(Self {
            objects: Arc::new(S3ObjectStore::new(client.clone())),
            client,
            catalog,
            bucket: config.s3_bucket.clone(),
            output_prefix: config.output_prefix.trim_matches('/').to_string(),
            spill_records: config.spill_records,
            leases,
            lease_ttl_events: config.lease_ttl_for(platform_archive::ArchiveKind::Events),
            lease_ttl_candles: config.lease_ttl_for(platform_archive::ArchiveKind::Candles),
            max_work: config.max_work,
            maintenance_safety: config.maintenance_safety,
            lease_initial_token: config.lease_initial_token,
            lease_clock_skew: config.lease_clock_skew,
        })
    }

    pub async fn compact(&self, batch: &SourceBatch) -> PlatformResult<CompactionStats> {
        let table = ArchiveIcebergTable::compacted(batch.kind);
        let digest = batch.digest();
        let scope = LeaseScope::new(self.lease_scope(batch));
        let scope_name = scope.as_str().to_string();
        let owner_id = Lease::new_owner_id();
        let initial_token = i64::try_from(self.lease_initial_token).unwrap_or(i64::MAX);
        let lease_ttl = match batch.kind {
            platform_archive::ArchiveKind::Events => self.lease_ttl_events,
            platform_archive::ArchiveKind::Candles => self.lease_ttl_candles,
        };
        let request = LeaseRequest::new(scope, &owner_id, lease_ttl)
            .with_initial_token(initial_token)?
            .with_clock_skew(self.lease_clock_skew)
            .with_context(table.name(), &digest);
        let Some(mut guard) =
            LeaseGuard::acquire(self.leases.clone(), request, self.max_work).await?
        else {
            MarketDataMetrics::record_lease_skip("compactor");
            MarketDataMetrics::record_compaction_outcome(batch.kind.label(), "lease_held");
            info!(
                table = table.name(),
                scope = %scope_name,
                owner = %owner_id,
                source_batch_digest = %digest,
                prefix = batch.prefix,
                "compaction skipped because another compactor holds the lease"
            );
            return Ok(CompactionStats {
                source_files: batch.files.len(),
                skipped: true,
                ..CompactionStats::default()
            });
        };
        let mut claim_skip = false;
        let result = async {
            guard.ensure_writable().await?;
            let uris = batch
                .files
                .iter()
                .map(|file| format!("s3://{}/{file}", self.bucket))
                .collect::<Vec<_>>();
            let claimed_uris = self.catalog.claimed_sources(table, &uris).await?;
            let claimed = batch
                .files
                .iter()
                .zip(&uris)
                .filter(|(_, uri)| claimed_uris.contains(*uri))
                .map(|(file, _)| file.clone())
                .collect::<Vec<_>>();
            if claimed.len() == uris.len() && self.catalog.supports_compaction_ledger() {
                let compaction_id =
                    CompactionId::for_batch(batch.kind, batch.bucket_start, &digest)?;
                if !self
                    .catalog
                    .compaction_recorded(batch.kind, compaction_id)
                    .await?
                {
                    let context = CommitContext::default()
                        .with_lease(
                            guard.lease().scope.as_str(),
                            &guard.lease().owner_id,
                            guard.lease().fencing_token,
                        )
                        .with_authorizer(guard.authorizer());
                    self.catalog
                        .record_compaction(
                            batch.kind,
                            compaction_id,
                            batch.bucket_start,
                            &uris,
                            platform_types::TimestampMilliseconds::now(),
                            &context,
                        )
                        .await?;
                }
            }
            if !claimed.is_empty() {
                warn!(
                    table = table.name(),
                    kind = batch.kind.label(),
                    prefix = %batch.prefix,
                    scope = %guard.lease().scope.as_str(),
                    owner = %guard.lease().owner_id,
                    token = guard.lease().fencing_token,
                    source_batch_digest = %digest,
                    claimed_sources = ?claimed,
                    "compaction skipped because the source set is already claimed"
                );
                claim_skip = true;
                return Ok(CompactionStats {
                    source_files: batch.files.len(),
                    skipped: true,
                    ..CompactionStats::default()
                });
            }
            guard.ensure_writable().await?;
            self.compact_leased(batch, &mut guard).await
        }
        .await;
        if let Err(error) = guard.release().await {
            warn!(%error, prefix = batch.prefix, "failed to release compaction lease");
        }
        let status = match &result {
            Ok(_) if claim_skip => "already_claimed",
            Ok(stats) if stats.skipped => "conflict",
            Ok(_) => "success",
            Err(_) => "error",
        };
        MarketDataMetrics::record_compaction_outcome(batch.kind.label(), status);
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
        let context = CommitContext::default()
            .with_lease(
                guard.lease().scope.as_str(),
                &guard.lease().owner_id,
                guard.lease().fencing_token,
            )
            .with_authorizer(guard.authorizer());
        let compaction_id =
            CompactionId::for_batch(batch.kind, batch.bucket_start, &batch.digest())?;
        let registration = ArchiveRegistration::new(data_files, Some(source_files.clone()))?
            .with_compaction_id(compaction_id);
        stats.skipped = self
            .register_compaction(batch, table, &registration, &context, guard)
            .await?;
        if !stats.skipped && self.catalog.supports_compaction_ledger() {
            self.catalog
                .record_compaction(
                    batch.kind,
                    compaction_id,
                    batch.bucket_start,
                    &source_files,
                    platform_types::TimestampMilliseconds::now(),
                    &context,
                )
                .await?;
        }

        Ok(stats)
    }

    async fn register_compaction(
        &self,
        batch: &SourceBatch,
        table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
        context: &CommitContext,
        guard: &LeaseGuard,
    ) -> PlatformResult<bool> {
        match self
            .catalog
            .register_files(table, registration, context)
            .await?
        {
            RegisterOutcome::Applied | RegisterOutcome::AlreadyApplied => Ok(false),
            RegisterOutcome::Conflict(conflict) => {
                warn!(
                    table = table.name(),
                    kind = batch.kind.label(),
                    prefix = %batch.prefix,
                    scope = %guard.lease().scope.as_str(),
                    owner = %guard.lease().owner_id,
                    token = guard.lease().fencing_token,
                    source_batch_digest = %batch.digest(),
                    claimed_sources = ?conflict.claimed_sources,
                    live_outputs = ?conflict.live_outputs,
                    "compaction registration conflicted"
                );
                Ok(true)
            }
        }
    }

    async fn maintain_claim_index(&self) -> PlatformResult<()> {
        for kind in [
            platform_archive::ArchiveKind::Events,
            platform_archive::ArchiveKind::Candles,
        ] {
            self.consolidate_claim_index(kind).await?;
        }
        for table in ArchiveIcebergTable::all() {
            let deleted = self
                .catalog
                .sweep_metadata(table, self.objects.as_ref(), self.maintenance_safety)
                .await?;
            if deleted > 0 {
                info!(
                    table = table.name(),
                    deleted, "removed unreachable archive metadata"
                );
            }
            if table.layer() != platform_archive::ArchiveTableLayer::Raw {
                let deleted = self
                    .catalog
                    .sweep_orphans(table, self.objects.as_ref(), self.maintenance_safety)
                    .await?;
                if deleted > 0 {
                    info!(
                        table = table.name(),
                        deleted, "removed unreachable archive data"
                    );
                }
            }
        }
        Ok(())
    }

    async fn consolidate_claim_index(
        &self,
        kind: platform_archive::ArchiveKind,
    ) -> PlatformResult<()> {
        let table = ArchiveIcebergTable::compacted(kind);
        let scope = LeaseScope::new(format!(
            "compact:{}:{}:{}:claim-index",
            self.bucket,
            self.output_prefix,
            kind.label()
        ));
        let owner_id = Lease::new_owner_id();
        let initial_token = i64::try_from(self.lease_initial_token).unwrap_or(i64::MAX);
        let lease_ttl = match kind {
            platform_archive::ArchiveKind::Events => self.lease_ttl_events,
            platform_archive::ArchiveKind::Candles => self.lease_ttl_candles,
        };
        let request = LeaseRequest::new(scope, &owner_id, lease_ttl)
            .with_initial_token(initial_token)?
            .with_clock_skew(self.lease_clock_skew)
            .with_context(table.name(), "claim-index");
        let Some(mut guard) =
            LeaseGuard::acquire(self.leases.clone(), request, self.max_work).await?
        else {
            MarketDataMetrics::record_lease_skip("compactor-maintenance");
            return Ok(());
        };
        let result = async {
            guard.ensure_writable().await?;
            let context = CommitContext::default()
                .with_lease(
                    guard.lease().scope.as_str(),
                    &guard.lease().owner_id,
                    guard.lease().fencing_token,
                )
                .with_authorizer(guard.authorizer());
            self.catalog.consolidate_claim_index(table, &context).await
        }
        .await;
        if let Err(error) = guard.release().await {
            warn!(%error, table = table.name(), "failed to release claim-index maintenance lease");
        }
        result
    }

    fn lease_scope(&self, batch: &SourceBatch) -> String {
        format!(
            "compact:{}:{}:{}:{}",
            self.bucket,
            self.output_prefix,
            batch.kind.label(),
            SourceBatch::prefix_digest(&batch.prefix)
        )
    }
}

#[async_trait]
impl BatchCompactor for Compactor {
    async fn compact(&self, batch: &SourceBatch) -> PlatformResult<CompactionStats> {
        Compactor::compact(self, batch).await
    }

    async fn maintain(&self) -> PlatformResult<()> {
        self.maintain_claim_index().await
    }
}
