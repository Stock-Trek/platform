use crate::iceberg::CompactedRewriteCandidate;
use crate::maintenance_executor::ArchiveMaintenanceExecutor;
use crate::{
    ArchiveIcebergTable, ArchiveKind, CommitContext, MaintenanceBlock, MaintenancePlan,
    StageOutcome,
};
use platform_errors::PlatformResult;
use platform_lease::CommitAuthorizer;
use platform_metrics::MarketDataMetrics;
use std::sync::Arc;

impl ArchiveMaintenanceExecutor {
    pub(crate) async fn apply_snapshots(
        &self,
        plan: MaintenancePlan,
        authorizer: Arc<dyn CommitAuthorizer>,
    ) -> PlatformResult<StageOutcome> {
        let context = self.context(authorizer);
        let mut applied = 0;
        for candidate in &plan.candidates {
            let table = ArchiveIcebergTable::all()
                .into_iter()
                .find(|table| table.name() == candidate.uri)
                .ok_or_else(|| Self::unknown_table(&candidate.uri))?;
            if !self.reserve_budget(0) {
                Self::record_budget_alert("expire_snapshots");
                return Ok(Self::budget_block(plan.stage, plan.candidates.len()));
            }
            applied += self
                .catalog
                .expire_archive_snapshots(table, self.config(), &context)
                .await?;
        }
        Ok(StageOutcome::completed(
            plan.stage,
            plan.candidates.len(),
            applied,
        ))
    }

    pub(crate) async fn apply_orphans(
        &self,
        plan: MaintenancePlan,
        _authorizer: Arc<dyn CommitAuthorizer>,
    ) -> PlatformResult<StageOutcome> {
        let now = Self::now();
        let selected =
            plan.deletion_candidates(now, self.config().orphan_grace, crate::MIN_ORPHAN_GRACE)?;
        let mut applied = 0;
        for table in ArchiveIcebergTable::all() {
            applied += self
                .catalog
                .sweep_metadata(table, self.objects.as_ref(), self.config().orphan_grace)
                .await?;
            applied += self
                .catalog
                .sweep_orphans(table, self.objects.as_ref(), self.config().orphan_grace)
                .await?;
        }
        tracing::info!(
            selected = selected.len(),
            "deleted unreachable archive objects"
        );
        Ok(StageOutcome::completed(
            plan.stage,
            plan.candidates.len(),
            applied,
        ))
    }

    pub(crate) async fn apply_rewrite(
        &self,
        plan: MaintenancePlan,
        authorizer: Arc<dyn CommitAuthorizer>,
    ) -> PlatformResult<StageOutcome> {
        let candidates = self.rewrite_candidates().await?;
        let context = self.context(authorizer);
        let mut applied = 0;
        for candidate in &candidates {
            let bytes = candidate
                .input_files
                .iter()
                .map(|file| u64::try_from(file.file_size_in_bytes).unwrap_or_default())
                .sum::<u64>();
            if !self.reserve_budget(bytes) {
                Self::record_budget_alert("compacted_rewrite");
                return Ok(Self::budget_block(plan.stage, plan.candidates.len()));
            }
            match self.rewrite_partition(candidate, &context).await {
                Some(outcome) => applied += outcome?,
                None => {
                    return Ok(StageOutcome::blocked(
                        plan.stage,
                        plan.candidates.len(),
                        MaintenanceBlock::new(
                            "partition_lease_contended",
                            "compacted rewrite skipped because another writer holds a partition lease",
                            false,
                        ),
                    ));
                }
            }
        }
        Ok(StageOutcome::completed(
            plan.stage,
            plan.candidates.len(),
            applied,
        ))
    }

    async fn rewrite_partition(
        &self,
        candidate: &CompactedRewriteCandidate,
        context: &CommitContext,
    ) -> Option<PlatformResult<usize>> {
        let partition = crate::MaintenancePartition::from_file_partition(candidate.partition);
        let mut guard = match self.partition_guard(&partition).await {
            Ok(Some(guard)) => guard,
            Ok(None) => {
                MarketDataMetrics::record_maintenance_alert(
                    "compacted_rewrite",
                    "partition_lease_contended",
                );
                MarketDataMetrics::record_lease_skip("archive-maintenance:partition");
                return None;
            }
            Err(error) => return Some(Err(error)),
        };
        if let Err(error) = guard.ensure_writable().await {
            let _ = guard.release().await;
            return Some(Err(error));
        }
        let result = self.catalog.rewrite_compacted(candidate, context).await;
        if let Err(error) = guard.release().await {
            tracing::warn!(%error, "failed to release the partition lease after rewrite");
        }
        Some(result)
    }

    pub(crate) async fn apply_raw_deletion(
        &self,
        plan: MaintenancePlan,
        authorizer: Arc<dyn CommitAuthorizer>,
    ) -> PlatformResult<StageOutcome> {
        if let Some(block) = self.metadata_growth_block().await? {
            return Ok(StageOutcome::blocked(
                plan.stage,
                plan.candidates.len(),
                block,
            ));
        }
        let now = Self::now();
        let context = self.context(authorizer);
        let mut applied = 0;
        for kind in ArchiveKind::all() {
            let raw = self
                .catalog
                .raw_cleanup_plan(kind, self.config(), now)
                .await?;
            if raw.partition_spec_changed {
                MarketDataMetrics::record_maintenance_alert(
                    "raw_deletion",
                    "partition_spec_changed",
                );
                return Ok(StageOutcome::blocked(
                    plan.stage,
                    plan.candidates.len(),
                    MaintenanceBlock::new(
                        "partition_spec_changed",
                        format!(
                            "raw deletion paused because {} ledger batches were written under a different partition spec; a re-baseline is required before raw objects expire",
                            kind.label()
                        ),
                        true,
                    ),
                ));
            }
            if !raw.quarantined.is_empty() {
                MarketDataMetrics::record_raw_expiry_decision("quarantine", kind.label());
                MarketDataMetrics::record_maintenance_alert(
                    "raw_deletion",
                    "source_manifest_quarantined",
                );
                return Ok(StageOutcome::blocked(
                    plan.stage,
                    plan.candidates.len(),
                    MaintenanceBlock::new(
                        "source_manifest_quarantined",
                        format!(
                            "raw deletion blocked because compaction batches are quarantined: {}",
                            raw.quarantined
                                .iter()
                                .map(|id| id.as_str())
                                .collect::<Vec<_>>()
                                .join(",")
                        ),
                        true,
                    ),
                ));
            }
            if raw.is_empty() {
                continue;
            }
            if !self.reserve_budget(0) {
                Self::record_budget_alert("raw_deletion");
                return Ok(Self::budget_block(plan.stage, plan.candidates.len()));
            }
            MarketDataMetrics::record_raw_expiry_decision("delete", kind.label());
            let outcome = raw
                .apply(self.catalog.as_ref(), self.objects.as_ref(), &context, now)
                .await?;
            applied += outcome.removed_entries + outcome.deleted_objects;
        }
        Ok(StageOutcome::completed(
            plan.stage,
            plan.candidates.len(),
            applied,
        ))
    }
}
