//! Archive catalog discovery for aggregator restore.
//!
//! Discovers the archive objects that make up a shard's recovery history
//! through the compacted/raw Iceberg handoff.

use super::replay::S3ArchivedEventSource;
use crate::config::ReplayStartPolicy;
use crate::shard::ShardId;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_archive::{
    ArchiveHandoff, ArchiveKind, ArchivePath, Bucket, IcebergCatalog, IcebergConfig, ObjectKey, S3,
};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use std::sync::Arc;
use tokio::sync::{OnceCell, Semaphore};
use tracing::info;

/// Maximum number of archive objects streamed concurrently across all
/// shards in one aggregator process.
const MAX_CONCURRENT_ARCHIVE_OBJECTS: usize = 4;

/// Archive used to rebuild shard state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRecoveryConfig {
    pub iceberg: IcebergConfig,
}

impl ArchiveRecoveryConfig {
    /// Reads the required archive recovery settings from a captured
    /// environment.
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            iceberg: IcebergConfig::from_vars(vars)?,
        })
    }

    /// Reads archive recovery settings when they are configured.
    ///
    /// Explicit `earliest` and `latest` starts intentionally discard persisted
    /// state, so they can run without an archive. When a catalog is provided,
    /// the rest of the catalog configuration is still validated.
    pub fn optional_from_vars(vars: &EnvVars) -> BpResult<Option<Self>> {
        IcebergConfig::optional_from_vars(vars)?
            .map_or(Ok(None), |iceberg| Ok(Some(Self { iceberg })))
    }

    /// Reads the archive recovery settings required by `policy` from a
    /// captured environment.
    ///
    /// `checkpoint` and `archive` require a fully configured archive. Explicit
    /// `earliest` and `latest` starts can omit it, but any settings that are
    /// present are still validated.
    pub fn from_vars_for_policy(
        vars: &EnvVars,
        policy: ReplayStartPolicy,
    ) -> BpResult<Option<Self>> {
        if policy.requires_archive() {
            Self::from_vars(vars).map(Some)
        } else {
            Self::optional_from_vars(vars)
        }
    }
}

/// Lazily connects to the archive and shares its clients and object listing
/// across every shard recovery source in this process.
///
/// The snapshot is created on the first shard that actually needs archive
/// replay, so checkpoints and live-only starts never touch S3. The listing is
/// startup-scoped; archive growth during recovery is not observed by later
/// shards.
pub struct S3ArchiveRecovery {
    config: ArchiveRecoveryConfig,
    snapshot: OnceCell<ArchiveSnapshot>,
    recovery_slots: Arc<Semaphore>,
}

struct ArchiveSnapshot {
    transfer_client: TransferClient,
    compacted_objects: usize,
    raw_tail_objects: usize,
    objects: Vec<ArchiveObject>,
}

/// An archive object with enough identity to stream it directly from S3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArchiveObject {
    pub(crate) bucket: Bucket,
    pub(crate) key: ObjectKey,
    pub(crate) size: i64,
}

impl ArchiveObject {
    fn from_data_file(file: &platform_archive::IcebergDataFile) -> BpResult<Self> {
        let (bucket, key) = S3::parse_uri(&file.file_path)?;
        Ok(Self {
            bucket,
            key,
            size: file.file_size_in_bytes,
        })
    }
}

impl S3ArchiveRecovery {
    pub fn new(config: ArchiveRecoveryConfig) -> Self {
        Self {
            config,
            snapshot: OnceCell::new(),
            recovery_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_ARCHIVE_OBJECTS)),
        }
    }

    /// Builds a shard-specific source from the shared archive snapshot.
    ///
    /// The first call discovers the archive files; later calls reuse the same
    /// snapshot and only select the objects owned by their shard. The snapshot
    /// is the compacted/raw handoff from the configured Iceberg catalog.
    pub async fn connect(
        &self,
        shard: ShardId,
        shard_count: usize,
    ) -> BpResult<S3ArchivedEventSource> {
        let snapshot = self
            .snapshot
            .get_or_try_init(|| async {
                let transfer_client = S3::transfer_client().await;
                let catalog = IcebergCatalog::connect(&self.config.iceberg).await?;
                let handoff = ArchiveHandoff::discover(&catalog, ArchiveKind::Events).await?;
                let compacted_objects = handoff.compacted.len();
                let raw_tail_objects = handoff.raw_tail.len();
                let objects = handoff
                    .files()
                    .map(ArchiveObject::from_data_file)
                    .collect::<BpResult<Vec<_>>>()?;

                Ok::<_, BpError>(ArchiveSnapshot {
                    transfer_client,
                    compacted_objects,
                    raw_tail_objects,
                    objects,
                })
            })
            .await?;

        let mut objects =
            ArchiveObject::select_for_shard(snapshot.objects.clone(), shard, shard_count);
        ArchiveObject::sort(&mut objects);

        info!(
            compacted_objects = snapshot.compacted_objects,
            raw_tail_objects = snapshot.raw_tail_objects,
            selected_objects = objects.len(),
            shard = %shard,
            "connected archive recovery source"
        );

        Ok(S3ArchivedEventSource::from_objects(
            snapshot.transfer_client.clone(),
            shard,
            shard_count,
            objects.into(),
            self.recovery_slots.clone(),
        ))
    }
}

impl ArchiveObject {
    /// Selects the event objects owned by one shard.
    ///
    /// Raw and compacted objects both carry a fixed virtual routing bucket, so a
    /// shard can discard every object belonging to another shard before
    /// downloading it. This keeps a cold rebuild at `O(archive)` across the
    /// cluster instead of making every shard scan the whole archive.
    pub(crate) fn select_for_shard(
        objects: Vec<ArchiveObject>,
        shard: ShardId,
        shard_count: usize,
    ) -> Vec<ArchiveObject> {
        objects
            .into_iter()
            .filter(|object| {
                let key = object.key.as_str();
                if ArchivePath::kind(key) != Some(ArchiveKind::Events) {
                    return false;
                }
                ArchivePath::routing_bucket_from_key(key)
                    .is_some_and(|bucket| bucket.is_owned_by(shard.get(), shard_count))
            })
            .collect()
    }

    /// Orders archive files by coverage hour, then by path.
    ///
    /// The layer prefix must not dominate ordering: an un-compacted older raw
    /// hour has to be replayed before a newer compacted hour so each shard
    /// observes its event history in time order.
    pub(crate) fn sort(objects: &mut [ArchiveObject]) {
        objects.sort_by(|left, right| {
            let left_hour = ArchivePath::bucket_start_from_key(left.key.as_str())
                .map(|hour| hour.0)
                .unwrap_or(i64::MAX);
            let right_hour = ArchivePath::bucket_start_from_key(right.key.as_str())
                .map(|hour| hour.0)
                .unwrap_or(i64::MAX);
            left_hour
                .cmp(&right_hour)
                .then_with(|| left.key.cmp(&right.key))
        });
    }
}
