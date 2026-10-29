use crate::config::CompactorConfig;
use async_trait::async_trait;
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchivePath, Bucket, ObjectKey, S3,
    SourceBatch,
};
use platform_errors::BpResult;
use platform_types::new_types::TimestampMilliseconds;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::warn;

/// Finds raw archive files that are ready to be compacted.
#[async_trait]
pub trait ArchiveBatchSource: Send + Sync {
    async fn pending_batches(&self) -> BpResult<Vec<SourceBatch>>;
}

/// Lists raw archive Parquet files from S3 and groups them by source folder.
///
/// A raw object is only compactable after the archiver has committed it to the
/// raw Iceberg table. Similarly, a raw object is skipped once a compacted table
/// snapshot records it as a processed source file.
pub struct S3ArchiveSource {
    client: aws_sdk_s3::Client,
    catalog: Arc<dyn ArchiveCatalog>,
    bucket: Bucket,
    prefix: String,
    settle_millis: i64,
    min_source_files: usize,
}

impl S3ArchiveSource {
    pub async fn connect(
        config: &CompactorConfig,
        catalog: Arc<dyn ArchiveCatalog>,
    ) -> BpResult<Self> {
        let (client, _) = S3::clients().await;
        let settle_millis = i64::try_from(config.settle.as_millis()).unwrap_or(i64::MAX);
        Ok(Self {
            client,
            catalog,
            bucket: Bucket::new(config.s3_bucket.clone()),
            prefix: config.s3_prefix.trim_matches('/').to_string(),
            settle_millis,
            min_source_files: config.min_source_files,
        })
    }

    async fn registered_raw_files(&self) -> BpResult<HashMap<ArchiveKind, HashSet<String>>> {
        let mut files = HashMap::new();
        for kind in [ArchiveKind::Events, ArchiveKind::Candles] {
            files.insert(
                kind,
                self.catalog
                    .registered_data_files(ArchiveIcebergTable::raw(kind))
                    .await?,
            );
        }
        Ok(files)
    }

    async fn processed_source_files(&self) -> BpResult<HashMap<ArchiveKind, HashSet<String>>> {
        let mut files = HashMap::new();
        for kind in [ArchiveKind::Events, ArchiveKind::Candles] {
            files.insert(
                kind,
                self.catalog
                    .processed_source_files(ArchiveIcebergTable::compacted(kind))
                    .await?,
            );
        }
        Ok(files)
    }
}

#[async_trait]
impl ArchiveBatchSource for S3ArchiveSource {
    async fn pending_batches(&self) -> BpResult<Vec<SourceBatch>> {
        let list_prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        let registered_raw = self.registered_raw_files().await?;
        let processed = self.processed_source_files().await?;
        let objects = S3::list_objects(
            &self.client,
            &self.bucket,
            &ObjectKey::new(list_prefix.clone()),
        )
        .await?;
        let mut groups: HashMap<String, PendingGroup> = HashMap::new();

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

            let s3_uri = format!("s3://{}/{key}", self.bucket);
            if !registered_raw
                .get(&kind)
                .is_some_and(|files| files.contains(&s3_uri))
            {
                continue;
            }
            if processed
                .get(&kind)
                .is_some_and(|files| files.contains(key))
            {
                continue;
            }

            let modified_at = object.last_modified_millis().unwrap_or(bucket_start.0);
            let group = groups
                .entry(parent.to_string())
                .or_insert_with(|| PendingGroup {
                    kind,
                    bucket_start,
                    latest_modified: modified_at,
                    files: Vec::new(),
                });
            group.latest_modified = group.latest_modified.max(modified_at);
            group.files.push(key.to_string());
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

struct PendingGroup {
    kind: ArchiveKind,
    bucket_start: TimestampMilliseconds,
    latest_modified: i64,
    files: Vec<String>,
}
