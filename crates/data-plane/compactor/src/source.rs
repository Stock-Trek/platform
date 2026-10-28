use crate::{config::CompactorConfig, record::ArchiveKind};
use aws_sdk_s3::{Client as S3Client, primitives::DateTime};
use chrono::{TimeZone, Utc};
use platform_errors::{BpError, BpResult};
use platform_types::new_types::TimestampMilliseconds;
use std::{collections::HashMap, str::FromStr};
use tracing::warn;

/// A group of raw archive files that can be compacted together.
#[derive(Debug)]
pub struct SourceBatch {
    pub kind: ArchiveKind,
    pub prefix: String,
    pub bucket_start: TimestampMilliseconds,
    pub files: Vec<String>,
}

/// Lists raw archive Parquet files from S3 and groups them by their source
/// folder.
///
/// TODO: track processed source files instead of rescanning every raw object on
/// each cycle.
pub struct S3ArchiveSource {
    client: S3Client,
    bucket: String,
    prefix: String,
    settle_millis: i64,
    min_source_files: usize,
}

impl S3ArchiveSource {
    pub async fn connect(config: &CompactorConfig) -> BpResult<Self> {
        let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let settle_millis = i64::try_from(config.settle.as_millis()).unwrap_or(i64::MAX);
        Ok(Self {
            client: S3Client::new(&sdk_config),
            bucket: config.s3_bucket.clone(),
            prefix: config.s3_prefix.trim_matches('/').to_string(),
            settle_millis,
            min_source_files: config.min_source_files,
        })
    }

    pub async fn pending_batches(&self) -> BpResult<Vec<SourceBatch>> {
        let list_prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        let mut request = self.client.list_objects_v2().bucket(&self.bucket);
        if !list_prefix.is_empty() {
            request = request.prefix(&list_prefix);
        }
        let mut paginator = request.into_paginator().send();
        let mut groups: HashMap<String, PendingGroup> = HashMap::new();

        while let Some(page) = paginator.try_next().await.map_err(|error| {
            BpError::s3(
                "list raw archive objects",
                format!("s3://{}/{}", self.bucket, list_prefix),
                error,
            )
        })? {
            for object in page.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                if !key.ends_with(".parquet") {
                    continue;
                }
                let Some(relative) = Self::relative_key(key, &list_prefix) else {
                    continue;
                };
                let Some(kind) = relative
                    .split('/')
                    .next()
                    .and_then(ArchiveKind::from_segment)
                else {
                    warn!(key, "skipping archive object with an unknown kind");
                    continue;
                };
                let Some(bucket_start) = Self::bucket_start(relative) else {
                    warn!(key, "skipping archive object outside the hourly layout");
                    continue;
                };
                let Some((parent, _)) = key.rsplit_once('/') else {
                    continue;
                };
                let modified_at = object
                    .last_modified()
                    .map(Self::modified_millis)
                    .unwrap_or(bucket_start.0);
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
        }

        let cutoff = Utc::now()
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

    fn relative_key<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
        if prefix.is_empty() {
            Some(key)
        } else {
            key.strip_prefix(prefix)
        }
    }

    fn bucket_start(relative: &str) -> Option<TimestampMilliseconds> {
        let year = Self::bucket_field::<i32>(relative, "year=")?;
        let month = Self::bucket_field::<u32>(relative, "month=")?;
        let day = Self::bucket_field::<u32>(relative, "day=")?;
        let hour = Self::bucket_field::<u32>(relative, "hour=")?;
        let bucket = Utc
            .with_ymd_and_hms(year, month, day, hour, 0, 0)
            .single()?;
        Some(TimestampMilliseconds(bucket.timestamp_millis()))
    }

    fn bucket_field<T>(relative: &str, name: &str) -> Option<T>
    where
        T: FromStr,
    {
        relative
            .split('/')
            .find_map(|segment| segment.strip_prefix(name))
            .and_then(|value| value.parse().ok())
    }

    fn modified_millis(value: &DateTime) -> i64 {
        value
            .secs()
            .saturating_mul(1_000)
            .saturating_add(i64::from(value.subsec_nanos() / 1_000_000))
    }
}

struct PendingGroup {
    kind: ArchiveKind,
    bucket_start: TimestampMilliseconds,
    latest_modified: i64,
    files: Vec<String>,
}
