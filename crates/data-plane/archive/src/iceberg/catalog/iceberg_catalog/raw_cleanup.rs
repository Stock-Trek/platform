use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::snapshot::SnapshotCommit;
use crate::iceberg::{ArchiveCatalog, ArchiveIcebergTable, ArchiveTableLayer, CommitContext};
use crate::{
    ArchiveKind, CompactionId, MaintenanceCandidate, MaintenanceConfig, MaintenanceFileKind,
    ObjectStore, S3,
};
use iceberg::spec::Operation;
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCleanupPlan {
    pub kind: ArchiveKind,
    pub raw_entries: Vec<String>,
    pub objects: Vec<String>,
    pub candidates: Vec<MaintenanceCandidate>,
    pub quarantined: Vec<CompactionId>,
    pub backlog: Option<Duration>,
    pub partition_spec_changed: bool,
}

impl RawCleanupPlan {
    pub fn is_empty(&self) -> bool {
        self.raw_entries.is_empty() && self.objects.is_empty()
    }

    pub fn objects(&self) -> usize {
        self.objects.len()
    }

    pub fn from_processed(
        kind: ArchiveKind,
        processed: HashMap<String, TimestampMilliseconds>,
        current_raw: &HashSet<String>,
        retained_raw: &HashSet<String>,
        now: TimestampMilliseconds,
        retention: Duration,
    ) -> Self {
        let retention_millis = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
        let backlog = processed
            .iter()
            .filter(|(source, _)| current_raw.contains(*source))
            .map(|(_, processed_at)| now.0.saturating_sub(processed_at.0))
            .min()
            .map(|millis| Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX)));
        let mut objects = Vec::new();
        let mut candidates = Vec::new();
        for (source, processed_at) in &processed {
            let due = processed_at.0.saturating_add(retention_millis) <= now.0;
            if due && !retained_raw.contains(source) {
                objects.push(source.clone());
                candidates.push(MaintenanceCandidate::new(
                    source.clone(),
                    kind,
                    MaintenanceFileKind::Raw,
                    *processed_at,
                ));
            }
        }
        candidates.sort_by(|left, right| left.uri.cmp(&right.uri));
        objects.sort();
        let mut raw_entries = objects
            .iter()
            .filter(|source| current_raw.contains(*source))
            .cloned()
            .collect::<Vec<_>>();
        raw_entries.sort();
        Self {
            kind,
            raw_entries,
            objects,
            candidates,
            quarantined: Vec::new(),
            backlog,
            partition_spec_changed: false,
        }
    }

    pub async fn apply(
        &self,
        catalog: &dyn ArchiveCatalog,
        store: &dyn ObjectStore,
        context: &CommitContext,
        now: TimestampMilliseconds,
    ) -> PlatformResult<RawCleanupOutcome> {
        let entries = self.raw_entries.iter().cloned().collect::<HashSet<_>>();
        let removed_entries = catalog
            .remove_raw_entries(ArchiveIcebergTable::raw(self.kind), &entries, context)
            .await?;
        let references = catalog.raw_table_references(self.kind).await?;
        let mut deleted_objects = 0;
        for object in &self.objects {
            if references.contains(object) {
                return Err(PlatformError::config_invalid(
                    "raw cleanup",
                    format!("raw object {object} is still referenced by a retained snapshot"),
                ));
            }
            let (bucket, key) = S3::parse_uri(object)?;
            store.delete(&bucket, &key).await?;
            deleted_objects += 1;
        }
        Ok(RawCleanupOutcome {
            removed_entries,
            deleted_objects,
            completed_at: now,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawCleanupOutcome {
    pub removed_entries: usize,
    pub deleted_objects: usize,
    pub completed_at: TimestampMilliseconds,
}

impl IcebergCatalog {
    pub async fn plan_raw_cleanup(
        &self,
        kind: ArchiveKind,
        config: &MaintenanceConfig,
        now: TimestampMilliseconds,
    ) -> PlatformResult<RawCleanupPlan> {
        config.validate()?;
        let retention = config.raw_retention_for(kind)?;
        let raw_table = self.ensure_table(ArchiveIcebergTable::raw(kind)).await?;
        let ledger_table = self.ensure_table(ArchiveIcebergTable::ledger(kind)).await?;
        let current_raw = self
            .data_files_from_table(&raw_table)
            .await?
            .into_iter()
            .map(|file| file.file_path)
            .collect::<HashSet<_>>();
        let retained_raw = Self::reachable_data_files(&raw_table).await?;
        let entries = self.read_ledger_entries(&ledger_table, kind).await?;
        let spec_id = ledger_table.metadata().default_partition_spec_id();
        let partition_spec_changed = entries
            .iter()
            .any(|entry| entry.partition_spec_id != spec_id);
        let mut processed = HashMap::new();
        let mut quarantined = Vec::new();
        for entry in &entries {
            let manifest = match self.read_source_manifest(&ledger_table, entry).await {
                Ok(manifest) => manifest,
                Err(error) => {
                    tracing::error!(
                        kind = kind.label(),
                        compaction_id = %entry.compaction_id.as_str(),
                        manifest = %entry.source_manifest_uri,
                        %error,
                        "quarantining compaction batch because its source manifest is missing or corrupt"
                    );
                    quarantined.push(entry.compaction_id);
                    continue;
                }
            };
            for source in manifest.sources {
                processed
                    .entry(source)
                    .and_modify(|processed_at: &mut TimestampMilliseconds| {
                        *processed_at = (*processed_at).max(entry.processed_at);
                    })
                    .or_insert(entry.processed_at);
            }
        }
        let mut plan = RawCleanupPlan::from_processed(
            kind,
            processed,
            &current_raw,
            &retained_raw,
            now,
            retention,
        );
        plan.quarantined = quarantined;
        plan.partition_spec_changed = partition_spec_changed;
        Ok(plan)
    }

    pub async fn raw_table_references(&self, kind: ArchiveKind) -> PlatformResult<HashSet<String>> {
        let table = self.ensure_table(ArchiveIcebergTable::raw(kind)).await?;
        Self::reachable_data_files(&table).await
    }

    pub async fn remove_raw_entries(
        &self,
        archive_table: ArchiveIcebergTable,
        entries: &HashSet<String>,
        context: &CommitContext,
    ) -> PlatformResult<usize> {
        if archive_table.layer() != ArchiveTableLayer::Raw {
            return Err(PlatformError::config_invalid(
                "raw table cleanup",
                "can only remove entries from a raw archive table",
            ));
        }
        if entries.is_empty() {
            return Ok(0);
        }
        let table = self
            .ensure_table_with_context(archive_table, Some(context))
            .await?;
        let mut removed = 0;
        let retained = self
            .data_files_from_table(&table)
            .await?
            .into_iter()
            .filter(|file| {
                if entries.contains(&file.file_path) {
                    removed += 1;
                    false
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
        if removed == 0 {
            return Ok(0);
        }
        let commit = SnapshotCommit::build_replacement_with_table_properties(
            &table,
            &retained,
            Operation::Replace,
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
        context.authorize().await?;
        self.send_table_commit(table.identifier(), &commit.to_request(table.identifier()))
            .await?;
        Ok(removed)
    }
}
