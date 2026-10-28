use crate::config::CompactorConfig;
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use platform_archive::{ArchiveKind, ArchivePath, Bucket, ObjectKey, S3, SourceBatch};
use platform_errors::{BpError, BpResult};
use platform_types::new_types::TimestampMilliseconds;
use std::collections::HashMap;
use tracing::warn;

/// Finds raw archive files that are ready to be compacted.
#[async_trait]
pub trait ArchiveBatchSource: Send + Sync {
    async fn pending_batches(&self) -> BpResult<Vec<SourceBatch>>;
}

/// Tracks which raw archive files have already been compacted.
#[async_trait]
pub trait ProcessedManifest: Send + Sync {
    /// The raw archive keys that were already compacted.
    async fn processed_files(&self) -> BpResult<std::collections::HashSet<String>>;

    /// Records a compacted batch so its files are never compacted again.
    async fn mark_processed(&self, files: &[String]) -> BpResult<()>;
}

/// Lists raw archive Parquet files from S3, skipping files already recorded in
/// the processed manifest, and groups the remainder by their source folder.
pub struct S3ArchiveSource {
    client: aws_sdk_s3::Client,
    bucket: Bucket,
    prefix: String,
    processed_prefix: String,
    settle_millis: i64,
    min_source_files: usize,
}

impl S3ArchiveSource {
    pub async fn connect(config: &CompactorConfig) -> BpResult<Self> {
        let (client, _) = S3::clients().await;
        let settle_millis = i64::try_from(config.settle.as_millis()).unwrap_or(i64::MAX);
        Ok(Self {
            client,
            bucket: Bucket::new(config.s3_bucket.clone()),
            prefix: config.s3_prefix.trim_matches('/').to_string(),
            processed_prefix: config.processed_prefix.trim_matches('/').to_string(),
            settle_millis,
            min_source_files: config.min_source_files,
        })
    }

    fn marker_key(&self, file: &str) -> String {
        if self.processed_prefix.is_empty() {
            file.to_string()
        } else {
            format!("{}/{}", self.processed_prefix, file)
        }
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
        let processed = Self::processed_files(self).await?;
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
            if processed.contains(key) {
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

        let cutoff = chrono::Utc::now()
            .timestamp_millis()
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

#[async_trait]
impl ProcessedManifest for S3ArchiveSource {
    async fn processed_files(&self) -> BpResult<std::collections::HashSet<String>> {
        let list_prefix = if self.processed_prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.processed_prefix)
        };
        let markers = S3::list_objects(
            &self.client,
            &self.bucket,
            &ObjectKey::new(list_prefix.clone()),
        )
        .await?;
        Ok(markers
            .into_iter()
            .filter_map(|object| {
                let key = object.key.as_str().to_string();
                key.strip_prefix(&list_prefix).map(str::to_string)
            })
            .collect())
    }

    async fn mark_processed(&self, files: &[String]) -> BpResult<()> {
        for file in files {
            let key = ObjectKey::new(self.marker_key(file));
            self.client
                .put_object()
                .bucket(self.bucket.as_str())
                .key(key.as_str())
                .body(ByteStream::from(Vec::new()))
                .send()
                .await
                .map_err(|error| {
                    BpError::s3(
                        "record processed archive file",
                        S3::object_target(&self.bucket, &key),
                        error,
                    )
                })?;
        }
        Ok(())
    }
}

struct PendingGroup {
    kind: ArchiveKind,
    bucket_start: TimestampMilliseconds,
    latest_modified: i64,
    files: Vec<String>,
}
