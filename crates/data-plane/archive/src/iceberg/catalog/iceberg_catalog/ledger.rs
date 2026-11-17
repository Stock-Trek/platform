use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::snapshot::SnapshotCommit;
use crate::iceberg::catalog::{IcebergDataFile, IcebergFilePartition};
use crate::iceberg::{ArchiveIcebergTable, CommitContext, RegisterOutcome};
use crate::{
    ArchiveKind, CompactionId, LEDGER_FORMAT_VERSION, LedgerEntry, LedgerParquet, SourceManifest,
    SourceManifestParquet,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::collections::{HashMap, HashSet};

impl IcebergCatalog {
    pub async fn record_compaction(
        &self,
        kind: ArchiveKind,
        compaction_id: CompactionId,
        bucket_start: TimestampMilliseconds,
        sources: &[String],
        processed_at: TimestampMilliseconds,
        context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        let table = self
            .ensure_table_with_context(ArchiveIcebergTable::ledger(kind), Some(context))
            .await?;
        if self.ledger_contains(&table, kind, compaction_id).await? {
            return Ok(RegisterOutcome::AlreadyApplied);
        }
        let manifest = SourceManifest::new(compaction_id, sources.to_vec())?;
        let manifest_uri = self.write_source_manifest(&table, &manifest).await?;
        let entry =
            LedgerEntry::from_manifest(kind, bucket_start, manifest_uri, &manifest, processed_at)?;
        self.append_ledger_entry(&table, &entry, context).await
    }

    pub async fn ledger_entries(&self, kind: ArchiveKind) -> PlatformResult<Vec<LedgerEntry>> {
        let table = self.ensure_table(ArchiveIcebergTable::ledger(kind)).await?;
        self.read_ledger_entries(&table, kind).await
    }

    pub async fn ledger_source_files(&self, kind: ArchiveKind) -> PlatformResult<HashSet<String>> {
        let table = self.ensure_table(ArchiveIcebergTable::ledger(kind)).await?;
        let entries = self.read_ledger_entries(&table, kind).await?;
        let mut sources = HashSet::new();
        for entry in entries {
            let input = table
                .file_io()
                .new_input(&entry.source_manifest_uri)
                .map_err(|error| Self::catalog_error("open source manifest", error))?;
            let manifest = SourceManifestParquet::decode(
                input
                    .read()
                    .await
                    .map_err(|error| Self::catalog_error("read source manifest", error))?,
            )?;
            if manifest.compaction_id != entry.compaction_id {
                return Err(PlatformError::external(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "source manifest {} does not match ledger compaction id {}",
                        entry.source_manifest_uri, entry.compaction_id
                    ),
                )));
            }
            sources.extend(manifest.sources);
        }
        Ok(sources)
    }

    pub async fn compaction_recorded(
        &self,
        kind: ArchiveKind,
        compaction_id: CompactionId,
    ) -> PlatformResult<bool> {
        let table = self.ensure_table(ArchiveIcebergTable::ledger(kind)).await?;
        self.ledger_contains(&table, kind, compaction_id).await
    }

    pub async fn read_ledger_entries(
        &self,
        table: &iceberg::table::Table,
        kind: ArchiveKind,
    ) -> PlatformResult<Vec<LedgerEntry>> {
        let mut by_id = HashMap::new();
        for file in self.data_files_from_table(table).await? {
            let input = table
                .file_io()
                .new_input(&file.file_path)
                .map_err(|error| Self::catalog_error("open ledger data file", error))?;
            let bytes = input
                .read()
                .await
                .map_err(|error| Self::catalog_error("read ledger data file", error))?;
            for entry in LedgerParquet::decode(bytes)? {
                if entry.kind != kind {
                    return Err(PlatformError::external(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "ledger row has the wrong archive kind",
                    )));
                }
                if entry.format_version > LEDGER_FORMAT_VERSION {
                    return Err(PlatformError::external(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "ledger row uses a newer format version",
                    )));
                }
                if let Some(existing) = by_id.insert(entry.compaction_id, entry.clone())
                    && existing != entry
                {
                    return Err(PlatformError::external(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("ledger has conflicting rows for {}", entry.compaction_id),
                    )));
                }
            }
        }
        let mut entries = by_id.into_values().collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.processed_at, entry.compaction_id));
        Ok(entries)
    }

    async fn ledger_contains(
        &self,
        table: &iceberg::table::Table,
        kind: ArchiveKind,
        compaction_id: CompactionId,
    ) -> PlatformResult<bool> {
        Ok(self
            .read_ledger_entries(table, kind)
            .await?
            .iter()
            .any(|entry| entry.compaction_id == compaction_id))
    }

    async fn write_source_manifest(
        &self,
        table: &iceberg::table::Table,
        manifest: &SourceManifest,
    ) -> PlatformResult<String> {
        let uri = format!(
            "{}/metadata/source-manifests/{}/{}.parquet",
            table.metadata().location(),
            manifest.compaction_id,
            manifest.format_version
        );
        let output = table
            .file_io()
            .new_output(&uri)
            .map_err(|error| Self::catalog_error("open source manifest output", error))?;
        output
            .write(SourceManifestParquet::encode(manifest)?)
            .await
            .map_err(|error| Self::catalog_error("write source manifest", error))?;
        Ok(uri)
    }

    async fn append_ledger_entry(
        &self,
        table: &iceberg::table::Table,
        entry: &LedgerEntry,
        context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        let uri = format!(
            "{}/data/{}/hour={}/processed-{}.parquet",
            table.metadata().location(),
            entry.kind.label(),
            entry.partition_hour_millis,
            entry.compaction_id
        );
        let bytes = LedgerParquet::encode_one(entry)?;
        let file = IcebergDataFile::partitioned(
            uri.clone(),
            i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            1,
            IcebergFilePartition::new(entry.kind, entry.partition_hour_millis),
        );
        let output = table
            .file_io()
            .new_output(&uri)
            .map_err(|error| Self::catalog_error("open ledger data output", error))?;
        output
            .write(bytes)
            .await
            .map_err(|error| Self::catalog_error("write ledger data file", error))?;
        let commit = SnapshotCommit::build_with_table_properties(
            table,
            &[file],
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
        let request = commit.to_request(table.identifier());
        context.authorize().await?;
        self.send_table_commit(table.identifier(), &request).await?;
        Ok(RegisterOutcome::Applied)
    }
}
