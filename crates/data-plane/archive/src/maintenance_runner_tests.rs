use crate::{
    MaintenanceBlock, MaintenanceConfig, MaintenancePlan, MaintenanceRunner, MaintenanceStage,
    MaintenanceStageExecutor, StageOutcome,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::PlatformResult;
use platform_lease::{CommitAuthorizer, InMemoryLeaseStore, LeaseRequest, LeaseScope, LeaseStore};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct FakeExecutor {
    stages: Mutex<Vec<MaintenanceStage>>,
    block: Mutex<Option<MaintenanceBlock>>,
    applied: Mutex<bool>,
}

#[async_trait]
impl MaintenanceStageExecutor for Arc<FakeExecutor> {
    async fn plan(&self, stage: MaintenanceStage) -> PlatformResult<MaintenancePlan> {
        self.stages.lock().push(stage);
        Ok(MaintenancePlan::dry_run(stage, Vec::new()))
    }

    async fn apply(
        &self,
        plan: MaintenancePlan,
        _authorizer: Arc<dyn CommitAuthorizer>,
    ) -> PlatformResult<StageOutcome> {
        *self.applied.lock() = true;
        if let Some(block) = self.block.lock().clone() {
            return Ok(StageOutcome::blocked(
                plan.stage,
                plan.candidates.len(),
                block,
            ));
        }
        Ok(StageOutcome::completed(
            plan.stage,
            plan.candidates.len(),
            plan.candidates.len(),
        ))
    }

    async fn stage_lag(&self, _stage: MaintenanceStage) -> PlatformResult<Duration> {
        Ok(Duration::from_secs(1))
    }
}

fn runner(
    executor: Arc<FakeExecutor>,
    store: Arc<InMemoryLeaseStore>,
) -> PlatformResult<MaintenanceRunner<Arc<FakeExecutor>>> {
    Ok(
        MaintenanceRunner::new(MaintenanceConfig::default(), executor, store)?
            .with_owner_id("maintenance-a"),
    )
}

#[tokio::test]
async fn dry_runs_plan_every_stage_without_applying() {
    let executor = Arc::new(FakeExecutor::default());
    let store = Arc::new(InMemoryLeaseStore::new());
    let runner = runner(executor.clone(), store).unwrap();

    let report = runner.run(true).await.unwrap();

    assert!(report.acquired);
    assert!(report.completed);
    assert_eq!(report.stages.len(), MaintenanceStage::all().len());
    assert_eq!(
        executor.stages.lock().as_slice(),
        MaintenanceStage::all().as_slice()
    );
    assert!(!*executor.applied.lock());
}

#[tokio::test]
async fn apply_runs_in_stage_order() {
    let executor = Arc::new(FakeExecutor::default());
    let store = Arc::new(InMemoryLeaseStore::new());
    let runner = runner(executor.clone(), store).unwrap();

    let report = runner.run(false).await.unwrap();

    assert!(report.completed);
    assert_eq!(report.stages.len(), 4);
    assert!(*executor.applied.lock());
}

#[tokio::test]
async fn stages_must_be_unique_and_ordered() {
    let executor = Arc::new(FakeExecutor::default());
    let store = Arc::new(InMemoryLeaseStore::new());
    let runner = runner(executor, store).unwrap();

    assert!(
        runner
            .run_stages(
                &[
                    MaintenanceStage::RawDeletion,
                    MaintenanceStage::ExpireSnapshots
                ],
                true
            )
            .await
            .is_err()
    );
    assert!(
        runner
            .run_stages(
                &[
                    MaintenanceStage::ExpireSnapshots,
                    MaintenanceStage::ExpireSnapshots
                ],
                true
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn blocked_stages_stop_later_stages() {
    let executor = Arc::new(FakeExecutor::default());
    *executor.block.lock() = Some(MaintenanceBlock::new("missing", "required file", true));
    let store = Arc::new(InMemoryLeaseStore::new());
    let runner = runner(executor.clone(), store).unwrap();

    let report = runner.run(false).await.unwrap();

    assert!(!report.completed);
    assert_eq!(report.stages.len(), 1);
    assert!(report.stages[0].is_blocked());
    assert_eq!(executor.stages.lock().len(), 1);
}

#[tokio::test]
async fn a_held_lease_blocks_maintenance() {
    let executor = Arc::new(FakeExecutor::default());
    let store = Arc::new(InMemoryLeaseStore::new());
    let scope = LeaseScope::new("archive-maintenance:archive");
    let lease = store
        .acquire(&LeaseRequest::new(scope, "other", Duration::from_secs(600)))
        .await
        .unwrap()
        .unwrap();
    let runner = runner(executor.clone(), store.clone()).unwrap();

    let report = runner.run(true).await.unwrap();

    assert!(!report.acquired);
    assert!(!report.completed);
    assert!(executor.stages.lock().is_empty());
    store.release(&lease).await.unwrap();
}
