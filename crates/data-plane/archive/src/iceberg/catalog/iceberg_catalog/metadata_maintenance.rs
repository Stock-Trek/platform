use super::claim_index::ClaimIndex;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::{
    ArchiveKind, ArchiveTableLayer, Bucket, MaintenanceCandidate, MaintenanceFileKind, ObjectKey,
    ObjectStore, S3, S3Object,
};
use iceberg::table::Table;
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;
use std::time::Duration;

impl IcebergCatalog {
    pub async fn metadata_file_count(
        &self,
        archive_table: crate::iceberg::ArchiveIcebergTable,
        client: &dyn ObjectStore,
    ) -> PlatformResult<u64> {
        let table = self.ensure_table(archive_table).await?;
        let directory = table
            .metadata()
            .metadata_location()
            .map_err(|error| Self::catalog_error("read Iceberg metadata directory", error))?;
        let (bucket, prefix) = Self::metadata_listing_prefix(&directory)?;
        let objects = client.list_all(&bucket, &prefix).await?;
        Ok(objects.len() as u64)
    }

    pub async fn orphan_candidates(
        &self,
        archive_table: crate::iceberg::ArchiveIcebergTable,
        client: &dyn ObjectStore,
    ) -> PlatformResult<Vec<MaintenanceCandidate>> {
        let table = self.ensure_table(archive_table).await?;
        let (bucket, prefix) = Self::listing_prefix(table.metadata().location())?;
        let objects = client.list_all(&bucket, &prefix).await?;
        let mut reachable = Self::reachable_metadata(&table).await?;
        reachable.extend(Self::reachable_data_files(&table).await?);
        reachable.extend(
            self.reachable_source_manifests(&table, archive_table)
                .await?,
        );
        let mut candidates =
            Self::orphan_file_candidates(&bucket, &objects, &reachable, archive_table, false);
        let directory = table
            .metadata()
            .metadata_location()
            .map_err(|error| Self::catalog_error("read Iceberg metadata directory", error))?;
        let (metadata_bucket, metadata_prefix) = Self::metadata_listing_prefix(&directory)?;
        let metadata_objects = client.list_all(&metadata_bucket, &metadata_prefix).await?;
        candidates.extend(Self::orphan_file_candidates(
            &metadata_bucket,
            &metadata_objects,
            &reachable,
            archive_table,
            true,
        ));
        candidates.sort_by(|left, right| left.uri.cmp(&right.uri));
        Ok(candidates)
    }

    fn orphan_file_candidates(
        bucket: &Bucket,
        objects: &[S3Object],
        reachable: &HashSet<String>,
        archive_table: crate::iceberg::ArchiveIcebergTable,
        metadata_directory: bool,
    ) -> Vec<MaintenanceCandidate> {
        objects
            .iter()
            .filter(|object| {
                object.last_modified_millis().is_some()
                    && !reachable.contains(&Self::object_uri(bucket, object))
            })
            .map(|object| {
                let uri = Self::object_uri(bucket, object);
                let file_kind = Self::candidate_file_kind(&uri, archive_table, metadata_directory);
                MaintenanceCandidate::new(
                    uri,
                    archive_table.kind(),
                    file_kind,
                    TimestampMilliseconds(object.last_modified_millis().unwrap_or_default()),
                )
            })
            .collect()
    }

    fn candidate_file_kind(
        uri: &str,
        archive_table: crate::iceberg::ArchiveIcebergTable,
        metadata_directory: bool,
    ) -> MaintenanceFileKind {
        if metadata_directory {
            if uri.contains("/source-manifests/") {
                return MaintenanceFileKind::SourceManifest;
            }
            return MaintenanceFileKind::Metadata;
        }
        match archive_table.layer() {
            ArchiveTableLayer::Raw => MaintenanceFileKind::Raw,
            ArchiveTableLayer::Compacted => MaintenanceFileKind::Compacted,
            ArchiveTableLayer::Ledger => MaintenanceFileKind::Ledger,
        }
    }

    pub async fn sweep_orphans(
        &self,
        table: &Table,
        archive_table: crate::iceberg::ArchiveIcebergTable,
        client: &dyn ObjectStore,
        safety_window: Duration,
    ) -> PlatformResult<usize> {
        let location = table.metadata().location();
        let (bucket, prefix) = Self::listing_prefix(location)?;
        let objects = client.list_all(&bucket, &prefix).await?;
        let mut reachable = Self::reachable_metadata(table).await?;
        reachable.extend(Self::reachable_data_files(table).await?);
        reachable.extend(
            self.reachable_source_manifests(table, archive_table)
                .await?,
        );
        let safety_millis = i64::try_from(safety_window.as_millis()).unwrap_or(i64::MAX);
        let cutoff =
            TimestampMilliseconds(TimestampMilliseconds::now().0.saturating_sub(safety_millis));
        let candidates =
            Self::metadata_maintenance_candidates(&bucket, &objects, &reachable, cutoff);
        let mut deleted = 0;
        for object in candidates {
            client.delete(&bucket, &object.key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    pub async fn sweep_metadata(
        &self,
        table: &Table,
        client: &dyn ObjectStore,
        safety_window: Duration,
    ) -> PlatformResult<usize> {
        let directory = table
            .metadata()
            .metadata_location()
            .map_err(|error| Self::catalog_error("read Iceberg metadata directory", error))?;
        let (bucket, prefix) = Self::metadata_listing_prefix(&directory)?;
        let objects = client.list_all(&bucket, &prefix).await?;
        let mut reachable = Self::reachable_metadata(table).await?;
        if let Some(archive_table) = Self::archive_table(table) {
            reachable.extend(
                self.reachable_source_manifests(table, archive_table)
                    .await?,
            );
        }
        let safety_millis = i64::try_from(safety_window.as_millis()).unwrap_or(i64::MAX);
        let cutoff =
            TimestampMilliseconds(TimestampMilliseconds::now().0.saturating_sub(safety_millis));
        let candidates =
            Self::metadata_maintenance_candidates(&bucket, &objects, &reachable, cutoff);
        let mut deleted = 0;
        for object in candidates {
            client.delete(&bucket, &object.key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    pub(super) async fn reachable_data_files(table: &Table) -> PlatformResult<HashSet<String>> {
        let mut reachable = HashSet::new();
        for snapshot in table.metadata().snapshots() {
            let manifests = table
                .manifest_list_reader(snapshot)
                .load()
                .await
                .map_err(|error| Self::catalog_error("read Iceberg manifest list", error))?;
            for manifest in manifests.entries() {
                let manifest = table
                    .manifest_reader()
                    .read(manifest)
                    .await
                    .map_err(|error| Self::catalog_error("read Iceberg manifest", error))?;
                for entry in manifest.entries() {
                    if entry.is_alive() {
                        reachable.insert(entry.file_path().to_string());
                    }
                }
            }
        }
        Ok(reachable)
    }

    async fn reachable_source_manifests(
        &self,
        table: &Table,
        archive_table: crate::iceberg::ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        if !archive_table.is_ledger() {
            return Ok(HashSet::new());
        }
        let files = Self::reachable_data_files(table).await?;
        Ok(self
            .read_ledger_entries_from_files(table, archive_table.kind(), files)
            .await?
            .into_iter()
            .map(|entry| entry.source_manifest_uri)
            .collect())
    }

    fn archive_table(table: &Table) -> Option<crate::iceberg::ArchiveIcebergTable> {
        let mut segments = table.identifier().name().split('/');
        let layer = segments.next()?;
        let kind = ArchiveKind::from_segment(segments.next()?)?;
        let layer = match layer {
            "raw" => ArchiveTableLayer::Raw,
            "compacted" => ArchiveTableLayer::Compacted,
            "ledger" => ArchiveTableLayer::Ledger,
            _ => return None,
        };
        Some(crate::iceberg::ArchiveIcebergTable::new(layer, kind))
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

    fn listing_prefix(location: &str) -> PlatformResult<(Bucket, ObjectKey)> {
        let (bucket, key) = S3::parse_uri(location)?;
        Ok((
            bucket,
            ObjectKey::new(format!("{}/", key.as_str().trim_end_matches('/'))),
        ))
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
