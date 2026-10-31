//! Archive catalog discovery for aggregator restore.
//!
//! Discovers the archive objects that make up a shard's recovery history,
//! either through the compacted/raw Iceberg handoff or by listing the raw
//! prefix.

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

pub const AGGREGATOR_ARCHIVE_S3_BUCKET: &str = "AGGREGATOR_ARCHIVE_S3_BUCKET";
pub const AGGREGATOR_ARCHIVE_S3_PREFIX: &str = "AGGREGATOR_ARCHIVE_S3_PREFIX";

/// Maximum number of archive objects streamed concurrently across all
/// shards in one aggregator process.
const MAX_CONCURRENT_ARCHIVE_OBJECTS: usize = 4;

/// Archive used to rebuild shard state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRecoveryConfig {
    pub bucket: String,
    pub prefix: String,
    /// When configured, recovery uses the compacted/raw handoff instead of
    /// replaying the full raw history.
    pub iceberg: Option<IcebergConfig>,
}

impl ArchiveRecoveryConfig {
    /// Reads the required archive recovery settings from the environment.
    ///
    /// The `checkpoint` and `archive` replay policies use this because they
    /// may need to rebuild state from the archive. Explicit `earliest` and
    /// `latest` starts use [`Self::optional_from_env`] instead.
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    /// Reads the required archive recovery settings from a captured
    /// environment.
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let bucket = vars.required(AGGREGATOR_ARCHIVE_S3_BUCKET)?;
        if bucket.trim().is_empty() {
            return Err(BpError::config_invalid(
                AGGREGATOR_ARCHIVE_S3_BUCKET,
                "must not be empty",
            ));
        }
        let prefix = vars.required(AGGREGATOR_ARCHIVE_S3_PREFIX)?;
        let iceberg = IcebergConfig::optional_from_vars(vars)?;
        Ok(Self {
            bucket,
            prefix,
            iceberg,
        })
    }

    /// Reads archive recovery settings when they are configured.
    ///
    /// Explicit `earliest` and `latest` starts intentionally discard persisted
    /// state, so they can run without an archive. When a bucket is provided,
    /// the rest of the archive configuration is still validated.
    pub fn optional_from_env() -> BpResult<Option<Self>> {
        Self::optional_from_vars(&EnvVars::from_env())
    }

    /// Reads optional archive recovery settings from a captured environment.
    pub fn optional_from_vars(vars: &EnvVars) -> BpResult<Option<Self>> {
        if vars.optional(AGGREGATOR_ARCHIVE_S3_BUCKET)?.is_none() {
            return Ok(None);
        }
        Self::from_vars(vars).map(Some)
    }

    /// Reads the archive recovery settings required by `policy`.
    ///
    /// `checkpoint` and `archive` require a fully configured archive. Explicit
    /// `earliest` and `latest` starts can omit it, but any settings that are
    /// present are still validated.
    pub fn from_env_for_policy(policy: ReplayStartPolicy) -> BpResult<Option<Self>> {
        Self::from_vars_for_policy(&EnvVars::from_env(), policy)
    }

    /// Reads the archive recovery settings required by `policy` from a
    /// captured environment.
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
    bucket: Bucket,
    prefix: String,
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
    /// snapshot and only select the objects owned by their shard. With an
    /// Iceberg catalog configured the snapshot is the compacted/raw handoff,
    /// otherwise it falls back to listing the raw prefix.
    pub async fn connect(
        &self,
        shard: ShardId,
        shard_count: usize,
    ) -> BpResult<S3ArchivedEventSource> {
        let snapshot = self
            .snapshot
            .get_or_try_init(|| async {
                let (client, transfer_client) = S3::clients().await;
                let bucket = Bucket::new(self.config.bucket.clone());
                let prefix = self.config.prefix.trim_matches('/').to_string();

                let (objects, compacted_objects, raw_tail_objects) = if let Some(iceberg) =
                    &self.config.iceberg
                {
                    let catalog = IcebergCatalog::connect(iceberg).await?;
                    let handoff = ArchiveHandoff::discover(&catalog, ArchiveKind::Events).await?;
                    let compacted_objects = handoff.compacted.len();
                    let raw_tail_objects = handoff.raw_tail.len();
                    let objects = handoff
                        .files()
                        .map(ArchiveObject::from_data_file)
                        .collect::<BpResult<Vec<_>>>()?;
                    (objects, compacted_objects, raw_tail_objects)
                } else {
                    let list_prefix = if prefix.is_empty() {
                        String::new()
                    } else {
                        format!("{prefix}/")
                    };
                    let listed =
                        S3::list_objects(&client, &bucket, &ObjectKey::new(list_prefix)).await?;
                    let raw_tail_objects = listed.len();
                    let objects = listed
                        .into_iter()
                        .map(|object| ArchiveObject {
                            bucket: bucket.clone(),
                            key: object.key,
                            size: object.size,
                        })
                        .collect();
                    (objects, 0, raw_tail_objects)
                };

                Ok::<_, BpError>(ArchiveSnapshot {
                    transfer_client,
                    bucket,
                    prefix,
                    compacted_objects,
                    raw_tail_objects,
                    objects,
                })
            })
            .await?;

        let mut objects = select_event_objects(snapshot.objects.clone(), shard, shard_count);
        sort_archive_objects(&mut objects);

        info!(
            bucket = %snapshot.bucket,
            prefix = %snapshot.prefix,
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

/// Selects the event objects owned by one shard.
///
/// Raw and compacted objects both carry a fixed virtual routing bucket, so a
/// shard can discard every object belonging to another shard before
/// downloading it. This keeps a cold rebuild at `O(archive)` across the cluster
/// instead of making every shard scan the whole archive.
pub(crate) fn select_event_objects(
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
pub(crate) fn sort_archive_objects(objects: &mut [ArchiveObject]) {
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
