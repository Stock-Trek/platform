use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::snapshot::SnapshotCommit;
use crate::iceberg::{
    ArchiveIcebergTable, ArchiveTableLayer, CommitContext, IcebergDataFile, IcebergFilePartition,
};
use crate::{
    ArchiveBatch, ArchiveHour, ArchiveParquet, ArchivePath, CompactionId, ContentHash, LedgerEntry,
    LedgerParquet, PartitionTarget,
};
use iceberg::spec::Operation;
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedRewriteCandidate {
    pub table: ArchiveIcebergTable,
    pub partition: IcebergFilePartition,
    pub input_files: Vec<IcebergDataFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedRewritePlan {
    pub candidates: Vec<CompactedRewriteCandidate>,
}

impl CompactedRewritePlan {
    pub fn from_files(
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        target: PartitionTarget,
    ) -> Self {
        let mut partitions = BTreeMap::new();
        for file in files {
            if let Some(partition) = Self::file_partition(table, file) {
                partitions
                    .entry((partition.kind.label(), partition.hour_millis))
                    .or_insert_with(Vec::new)
                    .push(file.clone());
            }
        }
        let candidates = partitions
            .into_values()
            .filter_map(|mut files| {
                files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
                let bytes = files
                    .iter()
                    .map(|file| u64::try_from(file.file_size_in_bytes).unwrap_or_default())
                    .sum();
                if !target.exceeded_by(files.len(), bytes) {
                    return None;
                }
                let mut selected = Vec::new();
                let mut selected_bytes = 0_u64;
                for file in files {
                    let bytes = u64::try_from(file.file_size_in_bytes).unwrap_or_default();
                    if !selected.is_empty()
                        && selected_bytes.saturating_add(bytes) > target.max_bytes
                    {
                        break;
                    }
                    selected_bytes = selected_bytes.saturating_add(bytes);
                    selected.push(file);
                }
                if selected.len() < 2 {
                    return None;
                }
                let partition = Self::file_partition(table, selected.first()?)?;
                Some(CompactedRewriteCandidate {
                    table,
                    partition,
                    input_files: selected,
                })
            })
            .collect();
        Self { candidates }
    }

    fn file_partition(
        table: ArchiveIcebergTable,
        file: &IcebergDataFile,
    ) -> Option<IcebergFilePartition> {
        if let Some(partition) = file.partition {
            return Some(partition);
        }
        if table.is_ledger() {
            let hour = file
                .file_path
                .split('/')
                .find_map(|segment| segment.strip_prefix("hour=")?.parse::<i64>().ok())?;
            return Some(IcebergFilePartition::new(table.kind(), hour));
        }
        let bucket = ArchivePath::parse_key(&file.file_path)?;
        Some(IcebergFilePartition::new(
            bucket.kind,
            bucket.bucket_start.0,
        ))
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    pub async fn apply(
        &self,
        catalog: &IcebergCatalog,
        context: &CommitContext,
    ) -> PlatformResult<usize> {
        let mut rewritten = 0;
        for candidate in &self.candidates {
            rewritten += candidate.apply(catalog, context).await?;
        }
        Ok(rewritten)
    }
}

impl CompactedRewriteCandidate {
    async fn apply(
        &self,
        catalog: &IcebergCatalog,
        context: &CommitContext,
    ) -> PlatformResult<usize> {
        if self.table.layer() == ArchiveTableLayer::Raw {
            return Err(PlatformError::config_invalid(
                "compacted rewrite",
                "cannot rewrite a raw archive table",
            ));
        }
        let table = catalog
            .ensure_table_with_context(self.table, Some(context))
            .await?;
        let current = catalog.data_files_from_table(&table).await?;
        let current_paths = current
            .iter()
            .map(|file| file.file_path.as_str())
            .collect::<std::collections::HashSet<_>>();
        if self
            .input_files
            .iter()
            .any(|file| !current_paths.contains(file.file_path.as_str()))
        {
            return Err(PlatformError::config_invalid(
                "compacted rewrite",
                "rewrite inputs are no longer part of the current snapshot",
            ));
        }

        let (bytes, record_count) = if self.table.is_ledger() {
            let entries = self.ledger_entries(&table).await?;
            (LedgerParquet::encode(&entries)?, entries.len())
        } else {
            let mut records = Vec::new();
            for file in &self.input_files {
                let input = table
                    .file_io()
                    .new_input(&file.file_path)
                    .map_err(|error| Self::catalog_error("open compacted data file", error))?;
                let bytes = input
                    .read()
                    .await
                    .map_err(|error| Self::catalog_error("read compacted data file", error))?;
                records.extend(ArchiveParquet::decode(self.partition.kind, bytes)?.into_records());
            }
            let batch = ArchiveBatch::new(self.partition.kind, records)?;
            let record_count = batch.len();
            (ArchiveParquet::encode(&batch)?.into(), record_count)
        };
        let hash = ContentHash::from_bytes(&bytes);
        let partition_path = if self.table.is_ledger() {
            format!("hour={}", self.partition.hour_millis)
        } else {
            ArchiveHour::from_timestamp(TimestampMilliseconds(self.partition.hour_millis))
                .ok_or_else(|| {
                    PlatformError::config_invalid(
                        "compacted rewrite",
                        "partition hour is outside the supported range",
                    )
                })?
                .path()
        };
        let uri = format!(
            "{}/data/{}/{partition_path}/rewrite-{hash}.parquet",
            table.metadata().location(),
            self.partition.kind.label(),
        );
        let output = table
            .file_io()
            .new_output(&uri)
            .map_err(|error| Self::catalog_error("open compacted rewrite output", error))?;
        let byte_count = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
        output
            .write(bytes)
            .await
            .map_err(|error| Self::catalog_error("write compacted rewrite output", error))?;
        let output_file = IcebergDataFile::partitioned(
            uri.clone(),
            byte_count,
            i64::try_from(record_count).unwrap_or(i64::MAX),
            self.partition,
        );
        let inputs = self
            .input_files
            .iter()
            .map(|file| file.file_path.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut retained = current
            .into_iter()
            .filter(|file| !inputs.contains(file.file_path.as_str()))
            .collect::<Vec<_>>();
        retained.push(output_file);
        let commit = SnapshotCommit::build_replacement_with_table_properties(
            &table,
            &retained,
            Operation::Replace,
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
        context.authorize().await?;
        catalog
            .send_table_commit(table.identifier(), &commit.to_request(table.identifier()))
            .await?;
        Ok(self.input_files.len())
    }

    async fn ledger_entries(
        &self,
        table: &iceberg::table::Table,
    ) -> PlatformResult<Vec<LedgerEntry>> {
        let mut by_id = BTreeMap::<CompactionId, LedgerEntry>::new();
        for file in &self.input_files {
            let input = table
                .file_io()
                .new_input(&file.file_path)
                .map_err(|error| Self::catalog_error("open ledger data file", error))?;
            let bytes = input
                .read()
                .await
                .map_err(|error| Self::catalog_error("read ledger data file", error))?;
            for entry in LedgerParquet::decode(bytes)? {
                if entry.kind != self.partition.kind {
                    return Err(PlatformError::config_invalid(
                        "ledger rewrite",
                        "ledger row has the wrong archive kind",
                    ));
                }
                if let Some(existing) = by_id.insert(entry.compaction_id, entry.clone())
                    && existing != entry
                {
                    return Err(PlatformError::config_invalid(
                        "ledger rewrite",
                        "ledger contains conflicting rows for one compaction id",
                    ));
                }
            }
        }
        Ok(by_id.into_values().collect())
    }

    fn catalog_error(operation: &'static str, error: iceberg::Error) -> PlatformError {
        IcebergCatalog::catalog_error(operation, error)
    }
}
