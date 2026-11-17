use crate::ArchiveKind;
use crate::iceberg::catalog::IcebergDataFile;
use crate::iceberg::catalog::IcebergFilePartition;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::claim_index::{ClaimIndex, ClaimState};
use futures::TryStreamExt;
use iceberg::spec::{Literal, PrimitiveLiteral, Struct};
use iceberg::table::Table;
use platform_errors::PlatformResult;
use platform_metrics::MarketDataMetrics;
use std::collections::HashSet;

impl IcebergCatalog {
    pub(super) async fn claim_state(&self, table: &Table) -> PlatformResult<ClaimState> {
        if ClaimIndex::has_head(table) {
            return ClaimIndex::load_cached(table, Some(&self.claim_index_cache)).await;
        }
        let table_name = match table.identifier().name() {
            "raw/events" => "raw/events",
            "raw/candles" => "raw/candles",
            "compacted/events" => "compacted/events",
            "compacted/candles" => "compacted/candles",
            _ => "unknown",
        };
        MarketDataMetrics::record_index_fallback_scan(table_name);
        let outputs = self.registered_paths(table).await?;
        let require_claims = table.identifier().name().starts_with("compacted/");
        let sources = Self::source_files_from_metadata(table, require_claims)?;
        Ok(ClaimState::new(sources, outputs))
    }

    pub(super) async fn claim_matches(
        &self,
        table: &Table,
        sources: &[String],
        outputs: &[String],
    ) -> PlatformResult<ClaimState> {
        if let Some(state) =
            ClaimIndex::find_cached(table, Some(&self.claim_index_cache), sources, outputs).await?
        {
            return Ok(state);
        }
        let claims = self.claim_state(table).await?;
        Ok(ClaimState::new(
            sources
                .iter()
                .filter(|source| claims.sources.contains(*source))
                .cloned(),
            outputs
                .iter()
                .filter(|output| claims.outputs.contains(*output))
                .cloned(),
        ))
    }

    pub async fn registered_paths(&self, table: &Table) -> PlatformResult<HashSet<String>> {
        let scan = table
            .scan()
            .build()
            .map_err(|error| Self::catalog_error("build Iceberg table scan", error))?;
        let mut tasks = scan
            .plan_files()
            .await
            .map_err(|error| Self::catalog_error("plan Iceberg table scan", error))?;
        let mut paths = HashSet::new();
        while let Some(task) = tasks
            .try_next()
            .await
            .map_err(|error| Self::catalog_error("read Iceberg table scan", error))?
        {
            paths.insert(task.data_file_path().to_string());
        }
        Ok(paths)
    }

    pub async fn data_files_from_table(
        &self,
        table: &Table,
    ) -> PlatformResult<Vec<IcebergDataFile>> {
        let scan = table
            .scan()
            .build()
            .map_err(|error| Self::catalog_error("build Iceberg table scan", error))?;
        let mut tasks = scan
            .plan_files()
            .await
            .map_err(|error| Self::catalog_error("plan Iceberg table scan", error))?;
        let mut files = Vec::new();
        while let Some(task) = tasks
            .try_next()
            .await
            .map_err(|error| Self::catalog_error("read Iceberg table scan", error))?
        {
            let partition = Self::file_partition(table, task.partition())?;
            let file = IcebergDataFile::new(
                task.data_file_path().to_string(),
                i64::try_from(task.file_size_in_bytes()).unwrap_or(i64::MAX),
                i64::try_from(task.record_count().unwrap_or_default()).unwrap_or(i64::MAX),
            );
            files.push(match partition {
                Some(partition) => IcebergDataFile::partitioned(
                    file.file_path,
                    file.file_size_in_bytes,
                    file.record_count,
                    partition,
                ),
                None => file,
            });
        }
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        Ok(files)
    }

    fn file_partition(
        table: &Table,
        values: Option<&Struct>,
    ) -> PlatformResult<Option<IcebergFilePartition>> {
        let spec = table.metadata().default_partition_spec();
        if spec.fields().is_empty() {
            return Ok(None);
        }
        let Some(values) = values else {
            return Ok(None);
        };
        let mut kind = None;
        let mut hour_millis = None;
        for (field, value) in spec.fields().iter().zip(values.fields()) {
            let Some(Literal::Primitive(value)) = value else {
                continue;
            };
            match (field.name.as_str(), value) {
                ("archive_kind", PrimitiveLiteral::String(value)) => {
                    kind = ArchiveKind::from_segment(value);
                }
                ("partition_hour_millis", PrimitiveLiteral::Long(value)) => {
                    hour_millis = Some(*value);
                }
                _ => {}
            }
        }
        Ok(kind
            .zip(hour_millis)
            .map(|(kind, hour_millis)| IcebergFilePartition::new(kind, hour_millis)))
    }
}
