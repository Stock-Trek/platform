use crate::maintenance_budget::MaintenanceBudget;
use crate::maintenance_partition::MaintenancePartition;
use crate::{
    ArchiveCatalog, CommitContext, MaintenanceConfig, MaintenancePlan, MaintenanceStage,
    MaintenanceStageExecutor, ObjectStore, StageOutcome,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::{CommitAuthorizer, LeaseGuard, LeaseRequest, LeaseStore};
use platform_types::TimestampMilliseconds;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

/// Production executor for the four-stage archive maintenance runner.
///
/// Plans and applies snapshot expiry, orphan cleanup, compacted and ledger
/// rewriting, and raw cleanup against an [`ArchiveCatalog`], coordinating with
/// compaction through partition-level leases. The executor is long-lived: it
/// tracks the hourly commit/byte budget and the per-table metadata baselines
/// between maintenance cycles, so one instance must be reused across cycles.
pub struct ArchiveMaintenanceExecutor {
    pub(crate) catalog: Arc<dyn ArchiveCatalog>,
    pub(crate) objects: Arc<dyn ObjectStore>,
    pub(crate) leases: Arc<dyn LeaseStore>,
    pub(crate) config: MaintenanceConfig,
    pub(crate) bucket: String,
    pub(crate) output_prefix: String,
    pub(crate) owner_id: String,
    pub(crate) backlogs: Mutex<BTreeMap<MaintenanceStage, Duration>>,
    pub(crate) budget: MaintenanceBudget,
    pub(crate) metadata_counts: Mutex<HashMap<String, u64>>,
}

impl ArchiveMaintenanceExecutor {
    pub fn new(
        catalog: Arc<dyn ArchiveCatalog>,
        objects: Arc<dyn ObjectStore>,
        leases: Arc<dyn LeaseStore>,
        config: MaintenanceConfig,
        bucket: impl Into<String>,
        output_prefix: impl Into<String>,
    ) -> PlatformResult<Self> {
        config.validate()?;
        config.validate()?;
        let budget = MaintenanceBudget::new(
            config.max_commits_per_hour,
            config.max_rewrite_bytes_per_hour,
        );
        Ok(Self {
            catalog,
            objects,
            leases,
            config,
            bucket: bucket.into(),
            output_prefix: output_prefix.into(),
            owner_id: platform_lease::Lease::new_owner_id(),
            backlogs: Mutex::new(BTreeMap::new()),
            budget,
            metadata_counts: Mutex::new(HashMap::new()),
        })
    }

    pub fn with_owner_id(mut self, owner_id: impl Into<String>) -> Self {
        self.owner_id = owner_id.into();
        self
    }

    pub(crate) fn config(&self) -> &MaintenanceConfig {
        &self.config
    }

    pub(crate) fn context(&self, authorizer: Arc<dyn CommitAuthorizer>) -> CommitContext {
        CommitContext::default().with_authorizer(authorizer)
    }

    fn record_backlog(&self, stage: MaintenanceStage, lag: Duration) {
        self.backlogs.lock().insert(stage, lag);
    }

    pub(crate) async fn partition_guard(
        &self,
        partition: &MaintenancePartition,
    ) -> PlatformResult<Option<LeaseGuard>> {
        let request = LeaseRequest::new(
            partition.lease_scope(&self.bucket, &self.output_prefix),
            self.owner_id.clone(),
            self.lease_ttl(),
        )
        .with_context(
            "archive-maintenance",
            format!("{}:{}", partition.kind.label(), partition.hour_millis),
        );
        LeaseGuard::acquire(self.leases.clone(), request, self.config.longest_lease).await
    }

    fn lease_ttl(&self) -> Duration {
        self.config
            .longest_lease
            .saturating_mul(2)
            .max(Duration::from_secs(60))
    }

    pub(crate) fn now() -> TimestampMilliseconds {
        TimestampMilliseconds::now()
    }

    pub(crate) fn unknown_table(table: &str) -> PlatformError {
        PlatformError::config_invalid(
            "archive maintenance",
            format!("unknown archive table {table}"),
        )
    }
}

#[async_trait]
impl MaintenanceStageExecutor for ArchiveMaintenanceExecutor {
    async fn plan(&self, stage: MaintenanceStage) -> PlatformResult<MaintenancePlan> {
        let (plan, lag) = match stage {
            MaintenanceStage::ExpireSnapshots => self.plan_snapshots().await?,
            MaintenanceStage::OrphanCleanup => self.plan_orphans().await?,
            MaintenanceStage::CompactedRewrite => self.plan_rewrite().await?,
            MaintenanceStage::RawDeletion => self.plan_raw_deletion().await?,
        };
        self.record_backlog(stage, lag);
        Ok(plan)
    }

    async fn apply(
        &self,
        plan: MaintenancePlan,
        authorizer: Arc<dyn CommitAuthorizer>,
    ) -> PlatformResult<StageOutcome> {
        match plan.stage {
            MaintenanceStage::ExpireSnapshots => self.apply_snapshots(plan, authorizer).await,
            MaintenanceStage::OrphanCleanup => self.apply_orphans(plan, authorizer).await,
            MaintenanceStage::CompactedRewrite => self.apply_rewrite(plan, authorizer).await,
            MaintenanceStage::RawDeletion => self.apply_raw_deletion(plan, authorizer).await,
        }
    }

    async fn stage_lag(&self, stage: MaintenanceStage) -> PlatformResult<Duration> {
        Ok(self
            .backlogs
            .lock()
            .get(&stage)
            .copied()
            .unwrap_or_default())
    }
}
