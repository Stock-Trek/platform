use crate::config::CompactorConfig;
use crate::object_lister::{ObjectLister, S3ObjectLister};
use crate::source_inventory::RawInventory;
use async_trait::async_trait;
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchivePath, Bucket, ObjectKey, S3,
    SourceBatch,
};
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::warn;

#[async_trait]
pub trait ArchiveBatchSource: Send + Sync {
    async fn pending_batches(&self) -> PlatformResult<Vec<SourceBatch>>;
}

pub struct S3ArchiveSource {
    pub(super) lister: Arc<dyn ObjectLister>,
    catalog: Arc<dyn ArchiveCatalog>,
    bucket: Bucket,
    prefix: String,
    settle_millis: i64,
    min_source_files: usize,
    pub(super) inventory: Mutex<RawInventory>,
}

impl S3ArchiveSource {
    #[cfg(test)]
    pub(crate) fn for_test(
        catalog: Arc<dyn ArchiveCatalog>,
        lister: Arc<dyn ObjectLister>,
        bucket: Bucket,
        prefix: String,
    ) -> Self {
        Self {
            lister,
            catalog,
            bucket,
            prefix,
            settle_millis: 0,
            min_source_files: 1,
            inventory: Mutex::new(RawInventory::default()),
        }
    }

    pub async fn connect(
        config: &CompactorConfig,
        catalog: Arc<dyn ArchiveCatalog>,
    ) -> PlatformResult<Self> {
        let lister = Arc::new(S3ObjectLister::new(S3::s3_client().await));
        let settle_millis = i64::try_from(config.settle.as_millis()).unwrap_or(i64::MAX);
        Ok(Self {
            lister,
            catalog,
            bucket: Bucket::new(config.s3_bucket.clone()),
            prefix: config.s3_prefix.trim_matches('/').to_string(),
            settle_millis,
            min_source_files: config.min_source_files,
            inventory: Mutex::new(RawInventory::default()),
        })
    }
}

#[async_trait]
impl ArchiveBatchSource for S3ArchiveSource {
    async fn pending_batches(&self) -> PlatformResult<Vec<SourceBatch>> {
        let list_prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        let prefix = ObjectKey::new(list_prefix.clone());
        self.refresh_inventory(&self.bucket, &prefix).await?;
        let objects = {
            let inventory = self.inventory.lock().await;
            inventory
                .candidate_keys
                .iter()
                .filter_map(|key| inventory.objects.get(key).cloned())
                .collect::<Vec<_>>()
        };
        let mut candidates: HashMap<ArchiveKind, Vec<Candidate>> = HashMap::new();

        for object in objects {
            let key = object.key.as_str();
            if !key.ends_with(".parquet") {
                continue;
            }
            let Some(relative) = ArchivePath::relative_key(key, &list_prefix) else {
                continue;
            };
            let Some(kind) = ArchivePath::kind(relative) else {
                warn!(key, "skipping archive object with an unknown kind");
                continue;
            };
            let Some(bucket_start) = ArchivePath::bucket_start_from_key(relative) else {
                warn!(key, "skipping archive object outside the hourly layout");
                continue;
            };
            let Some((parent, _)) = key.rsplit_once('/') else {
                continue;
            };
            candidates.entry(kind).or_default().push(Candidate {
                kind,
                parent: parent.to_string(),
                bucket_start,
                modified_at: object.last_modified_millis().unwrap_or(bucket_start.0),
                s3_uri: format!("s3://{}/{key}", self.bucket),
                key: key.to_string(),
            });
        }

        let mut completed_keys = Vec::new();
        let mut groups: HashMap<String, PendingGroup> = HashMap::new();
        for (kind, candidates) in candidates {
            let candidates = candidates
                .into_iter()
                .map(|candidate| (candidate.s3_uri.clone(), candidate))
                .collect::<HashMap<_, _>>();
            let uris = candidates.keys().cloned().collect::<Vec<_>>();
            let live = self
                .catalog
                .live_outputs(ArchiveIcebergTable::raw(kind), &uris)
                .await?;
            let claimed = self
                .catalog
                .claimed_sources(ArchiveIcebergTable::compacted(kind), &uris)
                .await?;
            for (uri, candidate) in candidates {
                if live.contains(&uri) && claimed.contains(&uri) {
                    completed_keys.push(candidate.key);
                    continue;
                }
                if !live.contains(&uri) {
                    continue;
                }
                let group = groups
                    .entry(candidate.parent)
                    .or_insert_with(|| PendingGroup {
                        kind: candidate.kind,
                        bucket_start: candidate.bucket_start,
                        latest_modified: candidate.modified_at,
                        files: Vec::new(),
                    });
                group.latest_modified = group.latest_modified.max(candidate.modified_at);
                group.files.push(candidate.key);
            }
        }

        {
            let mut inventory = self.inventory.lock().await;
            for key in completed_keys {
                inventory.candidate_keys.remove(&key);
            }
            let RawInventory {
                objects,
                candidate_keys,
                ..
            } = &mut *inventory;
            candidate_keys.retain(|key| objects.contains_key(key));
        }
        let cutoff = TimestampMilliseconds::now()
            .0
            .saturating_sub(self.settle_millis);
        let mut batches = groups
            .into_iter()
            .filter(|(_, group)| {
                group.files.len() >= self.min_source_files && group.latest_modified <= cutoff
            })
            .map(|(prefix, group)| SourceBatch {
                kind: group.kind,
                prefix,
                bucket_start: group.bucket_start,
                files: group.files,
            })
            .collect::<Vec<_>>();
        batches.sort_by_key(|batch| (batch.bucket_start.0, batch.prefix.clone()));
        Ok(batches)
    }
}

struct Candidate {
    kind: ArchiveKind,
    parent: String,
    bucket_start: TimestampMilliseconds,
    modified_at: i64,
    s3_uri: String,
    key: String,
}

struct PendingGroup {
    kind: ArchiveKind,
    bucket_start: TimestampMilliseconds,
    latest_modified: i64,
    files: Vec<String>,
}
