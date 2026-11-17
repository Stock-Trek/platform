use crate::iceberg::{ArchiveTableLayer, CompactedRewritePlan};
use crate::maintenance_executor::ArchiveMaintenanceExecutor;
use crate::{
    ArchiveIcebergTable, MaintenanceCandidate, MaintenanceFileKind, MaintenancePlan,
    MaintenanceStage, PartitionTarget,
};
use platform_errors::PlatformResult;
use platform_metrics::MarketDataMetrics;
use std::collections::BTreeMap;
use std::time::Duration;

impl ArchiveMaintenanceExecutor {
    pub(crate) async fn plan_snapshots(&self) -> PlatformResult<(MaintenancePlan, Duration)> {
        let now = Self::now();
        let candidates = ArchiveIcebergTable::all()
            .into_iter()
            .map(|table| {
                MaintenanceCandidate::new(
                    table.name().to_string(),
                    table.kind(),
                    MaintenanceFileKind::Metadata,
                    now,
                )
            })
            .collect();
        Ok((
            MaintenancePlan::dry_run(MaintenanceStage::ExpireSnapshots, candidates),
            Duration::ZERO,
        ))
    }

    pub(crate) async fn plan_orphans(&self) -> PlatformResult<(MaintenancePlan, Duration)> {
        let now = Self::now();
        let mut candidates = Vec::new();
        for table in ArchiveIcebergTable::all() {
            candidates.extend(
                self.catalog
                    .orphan_candidates(table, self.objects.as_ref())
                    .await?,
            );
        }
        let lag = candidates
            .iter()
            .map(|candidate| candidate.age(now))
            .max()
            .unwrap_or_default();
        Ok((
            MaintenancePlan::dry_run(MaintenanceStage::OrphanCleanup, candidates),
            lag,
        ))
    }

    pub(crate) async fn plan_rewrite(&self) -> PlatformResult<(MaintenancePlan, Duration)> {
        let now = Self::now();
        let mut candidates = Vec::new();
        for candidate in self.rewrite_candidates().await? {
            candidates.push(MaintenanceCandidate::new(
                format!(
                    "{}/{}/{}",
                    candidate.table.name(),
                    candidate.partition.kind.label(),
                    candidate.partition.hour_millis
                ),
                candidate.table.kind(),
                if candidate.table.layer() == ArchiveTableLayer::Ledger {
                    MaintenanceFileKind::Ledger
                } else {
                    MaintenanceFileKind::Compacted
                },
                now,
            ));
        }
        Ok((
            MaintenancePlan::dry_run(MaintenanceStage::CompactedRewrite, candidates),
            Duration::ZERO,
        ))
    }

    pub(crate) async fn rewrite_candidates(
        &self,
    ) -> PlatformResult<Vec<crate::iceberg::CompactedRewriteCandidate>> {
        let mut candidates = Vec::new();
        for table in ArchiveIcebergTable::all() {
            if table.layer() == ArchiveTableLayer::Raw {
                continue;
            }
            let files = self.catalog.data_files(table).await?;
            MarketDataMetrics::record_maintenance_partition_files(
                table.name(),
                Self::largest_partition_file_count(&files),
            );
            let rewrite =
                CompactedRewritePlan::from_files(table, &files, self.rewrite_target(table));
            candidates.extend(rewrite.candidates);
        }
        Ok(candidates)
    }

    pub(crate) async fn plan_raw_deletion(&self) -> PlatformResult<(MaintenancePlan, Duration)> {
        let now = Self::now();
        let mut candidates = Vec::new();
        let mut lag = Duration::ZERO;
        for kind in crate::ArchiveKind::all() {
            let plan = self
                .catalog
                .raw_cleanup_plan(kind, &self.config, now)
                .await?;
            MarketDataMetrics::record_maintenance_raw_backlog(
                kind.label(),
                plan.backlog.unwrap_or_default(),
            );
            if let Some(age) = plan.backlog {
                lag = lag.max(age);
            }
            candidates.extend(plan.candidates);
        }
        Ok((
            MaintenancePlan::dry_run(MaintenanceStage::RawDeletion, candidates),
            lag,
        ))
    }

    pub(crate) fn rewrite_target(&self, table: ArchiveIcebergTable) -> PartitionTarget {
        if table.layer() == ArchiveTableLayer::Ledger {
            self.config.ledger_target
        } else {
            self.config.compacted_target
        }
    }

    fn largest_partition_file_count(files: &[crate::IcebergDataFile]) -> u64 {
        let mut partitions = BTreeMap::new();
        for file in files {
            let Some(partition) = &file.partition else {
                continue;
            };
            *partitions
                .entry((partition.kind.label().to_string(), partition.hour_millis))
                .or_insert(0_u64) += 1;
        }
        partitions.into_values().max().unwrap_or_default()
    }
}
