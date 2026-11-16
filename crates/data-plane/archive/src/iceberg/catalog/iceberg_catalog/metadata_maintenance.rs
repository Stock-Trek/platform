use super::claim_index::ClaimIndex;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::{Bucket, ObjectKey, S3, S3Object};
use aws_sdk_s3::Client as S3Client;
use iceberg::table::Table;
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;
use std::time::Duration;

impl IcebergCatalog {
    pub async fn sweep_metadata(
        &self,
        table: &Table,
        client: &S3Client,
        safety_window: Duration,
    ) -> PlatformResult<usize> {
        let directory = table
            .metadata()
            .metadata_location()
            .map_err(|error| Self::catalog_error("read Iceberg metadata directory", error))?;
        let (bucket, prefix) = Self::metadata_listing_prefix(&directory)?;
        let objects = S3::list_objects(client, &bucket, &prefix).await?;
        let reachable = Self::reachable_metadata(table).await?;
        let safety_millis = i64::try_from(safety_window.as_millis()).unwrap_or(i64::MAX);
        let cutoff =
            TimestampMilliseconds(TimestampMilliseconds::now().0.saturating_sub(safety_millis));
        let candidates =
            Self::metadata_maintenance_candidates(&bucket, &objects, &reachable, cutoff);
        let mut deleted = 0;
        for object in candidates {
            S3::delete_object(client, &bucket, &object.key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    pub(super) async fn reachable_metadata(table: &Table) -> PlatformResult<HashSet<String>> {
        let mut reachable = ClaimIndex::reachable_locations(table).await?;
        if let Some(location) = table.metadata_location() {
            reachable.insert(location.to_string());
        }
        for entry in table.metadata().metadata_log() {
            reachable.insert(entry.metadata_file.clone());
        }
        for snapshot in table.metadata().snapshots() {
            reachable.insert(snapshot.manifest_list().to_string());
            let manifests = table
                .manifest_list_reader(snapshot)
                .load()
                .await
                .map_err(|error| Self::catalog_error("read Iceberg manifest list", error))?;
            for manifest in manifests.entries() {
                reachable.insert(manifest.manifest_path.clone());
            }
        }
        Ok(reachable)
    }

    pub(super) fn metadata_maintenance_candidates(
        bucket: &Bucket,
        objects: &[S3Object],
        reachable: &HashSet<String>,
        cutoff: TimestampMilliseconds,
    ) -> Vec<S3Object> {
        objects
            .iter()
            .filter(|object| {
                object
                    .last_modified_millis()
                    .is_some_and(|modified| modified < cutoff.0)
                    && !reachable.contains(&Self::object_uri(bucket, object))
            })
            .cloned()
            .collect()
    }

    fn object_uri(bucket: &Bucket, object: &S3Object) -> String {
        S3::object_target(bucket, &object.key)
    }

    pub(super) fn metadata_listing_prefix(location: &str) -> PlatformResult<(Bucket, ObjectKey)> {
        let (bucket, key) = S3::parse_uri(location)?;
        let Some((directory, _)) = key.as_str().rsplit_once('/') else {
            return Err(Self::catalog_error(
                "read Iceberg metadata directory",
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "metadata location has no parent directory",
                ),
            ));
        };
        Ok((bucket, ObjectKey::new(format!("{directory}/"))))
    }
}
