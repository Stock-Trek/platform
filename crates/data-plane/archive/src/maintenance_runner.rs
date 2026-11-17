use crate::maintenance_report::{MaintenanceBlock, MaintenanceRunReport, StageOutcome};
use crate::{MaintenanceConfig, MaintenancePlan, MaintenanceStage};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::{
    CommitAuthorizer, LeaseContext, LeaseGuard, LeaseRequest, LeaseScope, LeaseStore,
};
use platform_metrics::MarketDataMetrics;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

#[async_trait]
pub trait MaintenanceStageExecutor: Send + Sync {
    async fn plan(&self, stage: MaintenanceStage) -> PlatformResult<MaintenancePlan>;

    async fn apply(
        &self,
        plan: MaintenancePlan,
        authorizer: Arc<dyn CommitAuthorizer>,
    ) -> PlatformResult<StageOutcome>;

    async fn stage_lag(&self, _stage: MaintenanceStage) -> PlatformResult<Duration> {
        Ok(Duration::ZERO)
    }
}

pub struct MaintenanceRunner<E> {
    config: MaintenanceConfig,
    executor: E,
    leases: Arc<dyn LeaseStore>,
    owner_id: String,
    namespace: String,
}

impl<E> MaintenanceRunner<E>
where
    E: MaintenanceStageExecutor,
{
    pub fn new(
        config: MaintenanceConfig,
        executor: E,
        leases: Arc<dyn LeaseStore>,
    ) -> PlatformResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            executor,
            leases,
            owner_id: platform_lease::Lease::new_owner_id(),
            namespace: "archive".to_string(),
        })
    }

    pub fn with_owner_id(mut self, owner_id: impl Into<String>) -> Self {
        self.owner_id = owner_id.into();
        self
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn lease_scope(&self) -> LeaseScope {
        LeaseScope::new(format!("archive-maintenance:{}", self.namespace))
    }

    pub async fn run(&self, dry_run: bool) -> PlatformResult<MaintenanceRunReport> {
        self.run_stages(&MaintenanceStage::all(), dry_run).await
    }

    pub async fn run_stages(
        &self,
        stages: &[MaintenanceStage],
        dry_run: bool,
    ) -> PlatformResult<MaintenanceRunReport> {
        let report = self.run_stages_inner(stages, dry_run).await;
        match &report {
            Ok(report) if !report.acquired => {
                MarketDataMetrics::record_maintenance_run("skipped");
            }
            Ok(report) if report.dry_run => {
                MarketDataMetrics::record_maintenance_run("dry_run");
            }
            Ok(report) if report.completed => {
                MarketDataMetrics::record_maintenance_run("completed");
            }
            Ok(_) => {
                MarketDataMetrics::record_maintenance_run("blocked");
            }
            Err(_) => {
                MarketDataMetrics::record_maintenance_run("failed");
            }
        }
        report
    }

    async fn run_stages_inner(
        &self,
        stages: &[MaintenanceStage],
        dry_run: bool,
    ) -> PlatformResult<MaintenanceRunReport> {
        self.ensure_ordered(stages)?;
        let ttl = self.lease_ttl();
        let max_work = self.config.longest_lease;
        let request = LeaseRequest::new(self.lease_scope(), self.owner_id.clone(), ttl)
            .with_context("archive_maintenance", self.namespace.clone());
        let Some(mut guard) = LeaseGuard::acquire(self.leases.clone(), request, max_work).await?
        else {
            warn!(
                scope = %self.lease_scope().as_str(),
                "archive maintenance skipped because another runner holds the lease"
            );
            let mut report = MaintenanceRunReport::blocked(MaintenanceBlock::new(
                "lease_contended",
                "another archive maintenance runner holds the fencing lease",
                false,
            ));
            report.dry_run = dry_run;
            return Ok(report);
        };
        MarketDataMetrics::record_maintenance_lease_age(guard.lease().fencing_token);

        let mut report = MaintenanceRunReport {
            dry_run,
            acquired: true,
            completed: true,
            stages: Vec::new(),
        };
        for stage in stages {
            let plan = self.executor.plan(*stage).await?;
            let lag = self.executor.stage_lag(*stage).await?;
            MarketDataMetrics::record_maintenance_stage_lag(stage.label(), lag);
            if dry_run {
                info!(
                    stage = stage.label(),
                    candidates = plan.candidates.len(),
                    "archive maintenance dry run"
                );
                report
                    .stages
                    .push(StageOutcome::completed(*stage, plan.candidates.len(), 0));
                continue;
            }
            guard.ensure_writable().await?;
            let planned = plan.candidates.len();
            let outcome = self.executor.apply(plan, guard.authorizer()).await?;
            let blocked = outcome.blocked.clone();
            MarketDataMetrics::record_maintenance_stage(
                stage.label(),
                if blocked.is_some() {
                    "blocked"
                } else {
                    "completed"
                },
                outcome.applied as u64,
            );
            if let Some(block) = blocked {
                warn!(
                    stage = stage.label(),
                    code = %block.code,
                    permanent = block.permanent,
                    message = %block.message,
                    "archive maintenance stage blocked"
                );
                MarketDataMetrics::record_maintenance_alert(stage.label(), &block.code);
                report.completed = false;
                report
                    .stages
                    .push(StageOutcome::blocked(*stage, planned, block));
                break;
            }
            report.stages.push(outcome);
        }
        guard.release().await?;
        Ok(report)
    }

    fn ensure_ordered(&self, stages: &[MaintenanceStage]) -> PlatformResult<()> {
        if stages
            .windows(2)
            .any(|pair| pair[0].order() >= pair[1].order())
        {
            return Err(PlatformError::config_invalid(
                "archive maintenance stages",
                "must be unique and in expire, orphan, rewrite, raw order",
            ));
        }
        Ok(())
    }

    fn lease_ttl(&self) -> Duration {
        self.config
            .longest_lease
            .saturating_mul(2)
            .max(Duration::from_secs(60))
    }
}

impl<E> MaintenanceRunner<E>
where
    E: MaintenanceStageExecutor,
{
    pub fn lease_context(&self) -> LeaseContext {
        LeaseContext {
            table: Some("archive_maintenance".to_string()),
            source_batch_digest: Some(self.namespace.clone()),
        }
    }
}
