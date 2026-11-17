use crate::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, Bucket, CommitContext,
    CompactedRewriteCandidate, CompactionId, IcebergDataFile, IcebergFilePartition,
    MaintenanceCandidate, MaintenanceConfig, MaintenanceFileKind, MaintenancePartition,
    MaintenanceRunner, MaintenanceStage, MaintenanceStageExecutor, ObjectKey, ObjectStore,
    RawCleanupPlan, S3, S3ObjectPage, StageOutcome,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::{InMemoryLeaseStore, LeaseGuard, LeaseRequest, LeaseScope};
use platform_types::TimestampMilliseconds;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

const BUCKET: &str = "archive";
const PREFIX: &str = "compacted";
const HOUR: i64 = 1_718_467_200_000;
const DIGEST: &str = "3a6b3f6f6a4a4bd3caec6b7c3e5d2a1f0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f";

struct FakeObjects {
    deleted: Mutex<Vec<String>>,
}

impl FakeObjects {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            deleted: Mutex::new(Vec::new()),
        })
    }

    fn deleted(&self) -> Vec<String> {
        self.deleted.lock().clone()
    }
}

#[async_trait]
impl ObjectStore for FakeObjects {
    async fn list_page(
        &self,
        _bucket: &Bucket,
        _prefix: &ObjectKey,
        _continuation_token: Option<&str>,
        _max_keys: i32,
    ) -> PlatformResult<S3ObjectPage> {
        Ok(S3ObjectPage::default())
    }

    async fn delete(&self, bucket: &Bucket, key: &ObjectKey) -> PlatformResult<()> {
        self.deleted.lock().push(S3::object_target(bucket, key));
        Ok(())
    }
}

#[derive(Default)]
struct FakeCatalogState {
    calls: Vec<String>,
    expired: usize,
    removed_entries: usize,
    raw_plans: HashMap<ArchiveKind, RawCleanupPlan>,
}

struct FakeCatalog {
    state: Mutex<FakeCatalogState>,
    orphans: HashMap<ArchiveIcebergTable, Vec<MaintenanceCandidate>>,
    data_files: HashMap<ArchiveIcebergTable, Vec<IcebergDataFile>>,
    metadata_files: Mutex<u64>,
    metadata_growth: u64,
}

impl FakeCatalog {
    fn orphan_candidate(name: &str, age: Duration) -> MaintenanceCandidate {
        MaintenanceCandidate::new(
            name.to_string(),
            ArchiveKind::Events,
            MaintenanceFileKind::Raw,
            TimestampMilliseconds(TimestampMilliseconds::now().0 - age.as_millis() as i64),
        )
    }

    fn compacted_files() -> Vec<IcebergDataFile> {
        let bytes = 200 * 1024 * 1024;
        (0..3)
            .map(|index| {
                IcebergDataFile::partitioned(
                    format!("s3://{BUCKET}/{PREFIX}/events/{HOUR}/file-{index}.parquet"),
                    bytes,
                    1,
                    IcebergFilePartition::new(ArchiveKind::Events, HOUR),
                )
            })
            .collect()
    }

    fn raw_plan(entries: &[&str], backlog: Duration) -> RawCleanupPlan {
        let now = TimestampMilliseconds::now();
        RawCleanupPlan {
            kind: ArchiveKind::Events,
            raw_entries: entries.iter().map(|entry| entry.to_string()).collect(),
            objects: entries.iter().map(|entry| entry.to_string()).collect(),
            candidates: entries
                .iter()
                .map(|entry| {
                    MaintenanceCandidate::new(
                        entry.to_string(),
                        ArchiveKind::Events,
                        MaintenanceFileKind::Raw,
                        now,
                    )
                })
                .collect(),
            quarantined: Vec::new(),
            partition_spec_changed: false,
            backlog: Some(backlog),
        }
    }
}

#[async_trait]
impl ArchiveCatalog for FakeCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        _registration: &crate::ArchiveRegistration,
        _context: &CommitContext,
    ) -> PlatformResult<crate::RegisterOutcome> {
        Err(unsupported())
    }

    async fn registered_data_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(HashSet::new())
    }

    async fn processed_source_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(HashSet::new())
    }

    async fn data_files(&self, table: ArchiveIcebergTable) -> PlatformResult<Vec<IcebergDataFile>> {
        Ok(self.data_files.get(&table).cloned().unwrap_or_default())
    }

    async fn sweep_metadata(
        &self,
        _table: ArchiveIcebergTable,
        _client: &dyn ObjectStore,
        _safety_window: Duration,
    ) -> PlatformResult<usize> {
        self.state.lock().calls.push("sweep_metadata".to_string());
        Ok(1)
    }

    async fn sweep_orphans(
        &self,
        _table: ArchiveIcebergTable,
        _client: &dyn ObjectStore,
        _safety_window: Duration,
    ) -> PlatformResult<usize> {
        self.state.lock().calls.push("sweep_orphans".to_string());
        Ok(2)
    }

    async fn expire_archive_snapshots(
        &self,
        table: ArchiveIcebergTable,
        _config: &MaintenanceConfig,
        _context: &CommitContext,
    ) -> PlatformResult<usize> {
        let mut state = self.state.lock();
        state.calls.push(format!("expire:{}", table.name()));
        state.expired += 1;
        Ok(3)
    }

    async fn orphan_candidates(
        &self,
        table: ArchiveIcebergTable,
        _client: &dyn ObjectStore,
    ) -> PlatformResult<Vec<MaintenanceCandidate>> {
        Ok(self.orphans.get(&table).cloned().unwrap_or_default())
    }

    async fn rewrite_compacted(
        &self,
        candidate: &CompactedRewriteCandidate,
        _context: &CommitContext,
    ) -> PlatformResult<usize> {
        let mut state = self.state.lock();
        state.calls.push(format!(
            "rewrite:{}:{}",
            candidate.table.name(),
            candidate.partition.hour_millis
        ));
        Ok(candidate.input_files.len())
    }

    async fn raw_cleanup_plan(
        &self,
        kind: ArchiveKind,
        _config: &MaintenanceConfig,
        _now: TimestampMilliseconds,
    ) -> PlatformResult<RawCleanupPlan> {
        let mut state = self.state.lock();
        state.calls.push("raw_plan".to_string());
        Ok(state
            .raw_plans
            .get(&kind)
            .cloned()
            .unwrap_or(RawCleanupPlan {
                kind,
                raw_entries: Vec::new(),
                objects: Vec::new(),
                candidates: Vec::new(),
                quarantined: Vec::new(),
                partition_spec_changed: false,
                backlog: None,
            }))
    }

    async fn raw_table_references(&self, _kind: ArchiveKind) -> PlatformResult<HashSet<String>> {
        Ok(HashSet::new())
    }

    async fn metadata_file_count(
        &self,
        _table: ArchiveIcebergTable,
        _client: &dyn ObjectStore,
    ) -> PlatformResult<u64> {
        let mut count = self.metadata_files.lock();
        let current = *count;
        *count += self.metadata_growth;
        Ok(current)
    }

    async fn remove_raw_entries(
        &self,
        _table: ArchiveIcebergTable,
        entries: &HashSet<String>,
        _context: &CommitContext,
    ) -> PlatformResult<usize> {
        let mut state = self.state.lock();
        state.calls.push("remove_raw_entries".to_string());
        state.removed_entries += entries.len();
        Ok(entries.len())
    }
}

fn unsupported() -> PlatformError {
    PlatformError::external(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "unsupported",
    ))
}

fn executor(
    catalog: Arc<FakeCatalog>,
    objects: Arc<FakeObjects>,
    leases: Arc<InMemoryLeaseStore>,
) -> PlatformResult<crate::ArchiveMaintenanceExecutor> {
    crate::ArchiveMaintenanceExecutor::new(
        catalog,
        objects,
        leases,
        MaintenanceConfig::default(),
        BUCKET,
        PREFIX,
    )
    .map(|executor| executor.with_owner_id("maintenance-a"))
}

fn partition_scope(kind: ArchiveKind, hour: i64) -> String {
    MaintenancePartition::new(kind, hour)
        .lease_scope(BUCKET, PREFIX)
        .as_str()
        .to_string()
}

fn outcome(report: &crate::MaintenanceRunReport, stage: MaintenanceStage) -> StageOutcome {
    report
        .stages
        .iter()
        .find(|outcome| outcome.stage == stage)
        .unwrap()
        .clone()
}

#[tokio::test]
async fn applies_all_four_stages_in_order() -> PlatformResult<()> {
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState::default()),
        orphans: HashMap::new(),
        data_files: HashMap::from([(
            ArchiveIcebergTable::CompactedEvents,
            FakeCatalog::compacted_files(),
        )]),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = executor(catalog.clone(), objects.clone(), leases.clone())?;
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?
        .with_owner_id("maintenance-a");

    let report = runner.run(false).await?;

    assert!(report.acquired);
    assert!(report.completed);
    let state = catalog.state.lock();
    assert_eq!(state.expired, 6);
    assert!(state.calls.contains(&format!(
        "rewrite:{}:{}",
        ArchiveIcebergTable::CompactedEvents.name(),
        HOUR
    )));
    assert_eq!(state.removed_entries, 0);
    assert!(objects.deleted().is_empty());
    for stage in MaintenanceStage::all() {
        assert!(outcome(&report, stage).blocked.is_none(), "{stage:?}");
    }
    Ok(())
}

#[tokio::test]
async fn dry_run_plans_without_applying() -> PlatformResult<()> {
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState::default()),
        orphans: HashMap::from([(
            ArchiveIcebergTable::RawEvents,
            vec![FakeCatalog::orphan_candidate(
                "s3://archive/raw/events/old.parquet",
                Duration::from_secs(90 * 24 * 60 * 60),
            )],
        )]),
        data_files: HashMap::from([(
            ArchiveIcebergTable::CompactedEvents,
            FakeCatalog::compacted_files(),
        )]),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = executor(catalog.clone(), objects.clone(), leases.clone())?;
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?;

    let report = runner.run(true).await?;

    assert!(report.dry_run);
    let state = catalog.state.lock();
    assert_eq!(state.expired, 0);
    assert!(state.calls.iter().all(|call| call == "raw_plan"));
    assert!(objects.deleted().is_empty());
    assert_eq!(outcome(&report, MaintenanceStage::OrphanCleanup).planned, 1);
    Ok(())
}

#[tokio::test]
async fn rewrite_blocked_when_partition_lease_is_held() -> PlatformResult<()> {
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState::default()),
        orphans: HashMap::new(),
        data_files: HashMap::from([(
            ArchiveIcebergTable::CompactedEvents,
            FakeCatalog::compacted_files(),
        )]),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let holder = LeaseGuard::acquire(
        leases.clone(),
        LeaseRequest::new(
            LeaseScope::new(partition_scope(ArchiveKind::Events, HOUR)),
            "writer",
            Duration::from_secs(3600),
        ),
        Duration::from_secs(1800),
    )
    .await?
    .unwrap();
    let maintenance = executor(catalog.clone(), objects.clone(), leases.clone())?;
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?;

    let report = runner.run(false).await?;

    drop(holder);
    let rewrite = outcome(&report, MaintenanceStage::CompactedRewrite);
    assert_eq!(
        rewrite.blocked.as_ref().unwrap().code,
        "partition_lease_contended"
    );
    assert!(!rewrite.blocked.as_ref().unwrap().permanent);
    assert!(!report.completed);
    assert!(
        report
            .stages
            .iter()
            .all(|stage| stage.stage != MaintenanceStage::RawDeletion)
    );
    assert!(
        catalog
            .state
            .lock()
            .calls
            .iter()
            .all(|call| !call.starts_with("rewrite:"))
    );
    Ok(())
}

#[tokio::test]
async fn raw_deletion_blocked_when_manifest_is_quarantined() -> PlatformResult<()> {
    let mut plan = FakeCatalog::raw_plan(&["s3://archive/raw/events/a.parquet"], Duration::ZERO);
    plan.quarantined.push(
        CompactionId::for_batch(ArchiveKind::Events, TimestampMilliseconds::now(), DIGEST).unwrap(),
    );
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState {
            raw_plans: HashMap::from([(ArchiveKind::Events, plan)]),
            ..FakeCatalogState::default()
        }),
        orphans: HashMap::new(),
        data_files: HashMap::new(),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = executor(catalog.clone(), objects.clone(), leases.clone())?;
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?;

    let report = runner.run(false).await?;

    let raw = outcome(&report, MaintenanceStage::RawDeletion);
    assert_eq!(
        raw.blocked.as_ref().unwrap().code,
        "source_manifest_quarantined"
    );
    assert!(raw.blocked.as_ref().unwrap().permanent);
    assert_eq!(catalog.state.lock().removed_entries, 0);
    assert!(objects.deleted().is_empty());
    Ok(())
}

#[tokio::test]
async fn removes_entries_and_deletes_objects() -> PlatformResult<()> {
    let object = "s3://archive/raw/events/a.parquet";
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState {
            raw_plans: HashMap::from([(
                ArchiveKind::Events,
                FakeCatalog::raw_plan(&[object], Duration::from_secs(400 * 24 * 60 * 60)),
            )]),
            ..FakeCatalogState::default()
        }),
        orphans: HashMap::new(),
        data_files: HashMap::new(),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = executor(catalog.clone(), objects.clone(), leases.clone())?;
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?;

    let report = runner.run(false).await?;

    let raw = outcome(&report, MaintenanceStage::RawDeletion);
    assert!(raw.blocked.is_none());
    assert_eq!(catalog.state.lock().removed_entries, 1);
    assert_eq!(objects.deleted(), vec![object.to_string()]);
    Ok(())
}

#[tokio::test]
async fn raw_deletion_pauses_when_the_partition_spec_changes() -> PlatformResult<()> {
    let mut plan = FakeCatalog::raw_plan(
        &["s3://archive/raw/events/a.parquet"],
        Duration::from_secs(400 * 24 * 60 * 60),
    );
    plan.partition_spec_changed = true;
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState {
            raw_plans: HashMap::from([(ArchiveKind::Events, plan)]),
            ..FakeCatalogState::default()
        }),
        orphans: HashMap::new(),
        data_files: HashMap::new(),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = executor(catalog.clone(), objects.clone(), leases.clone())?;
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?;

    let report = runner.run(false).await?;

    let raw = outcome(&report, MaintenanceStage::RawDeletion);
    assert_eq!(raw.blocked.as_ref().unwrap().code, "partition_spec_changed");
    assert!(raw.blocked.as_ref().unwrap().permanent);
    assert_eq!(catalog.state.lock().removed_entries, 0);
    assert!(objects.deleted().is_empty());
    Ok(())
}

#[tokio::test]
async fn raw_deletion_pauses_when_metadata_growth_exceeds_the_budget() -> PlatformResult<()> {
    let config = MaintenanceConfig {
        max_metadata_growth_percent: 1,
        ..MaintenanceConfig::default()
    };
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(100),
        metadata_growth: 50,
        state: Mutex::new(FakeCatalogState::default()),
        orphans: HashMap::new(),
        data_files: HashMap::new(),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = crate::ArchiveMaintenanceExecutor::new(
        catalog.clone(),
        objects.clone(),
        leases.clone(),
        config.clone(),
        BUCKET,
        PREFIX,
    )?
    .with_owner_id("maintenance-a");
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?;

    runner.run(false).await?;
    let report = runner.run(false).await?;

    let raw = outcome(&report, MaintenanceStage::RawDeletion);
    assert_eq!(
        raw.blocked.as_ref().unwrap().code,
        "metadata_growth_exceeded"
    );
    assert!(!raw.blocked.as_ref().unwrap().permanent);
    assert_eq!(catalog.state.lock().removed_entries, 0);
    Ok(())
}

#[tokio::test]
async fn the_hourly_budget_blocks_snapshot_expiry() -> PlatformResult<()> {
    let config = MaintenanceConfig {
        max_commits_per_hour: 1,
        ..MaintenanceConfig::default()
    };
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState::default()),
        orphans: HashMap::new(),
        data_files: HashMap::from([(
            ArchiveIcebergTable::CompactedEvents,
            FakeCatalog::compacted_files(),
        )]),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = crate::ArchiveMaintenanceExecutor::new(
        catalog.clone(),
        objects.clone(),
        leases.clone(),
        config,
        BUCKET,
        PREFIX,
    )?
    .with_owner_id("maintenance-a");
    let runner = MaintenanceRunner::new(MaintenanceConfig::default(), maintenance, leases.clone())?;

    let report = runner.run(false).await?;

    let snapshots = outcome(&report, MaintenanceStage::ExpireSnapshots);
    assert_eq!(
        snapshots.blocked.as_ref().unwrap().code,
        "maintenance_budget_exhausted"
    );
    assert!(!snapshots.blocked.as_ref().unwrap().permanent);
    assert!(!report.completed);
    assert_eq!(catalog.state.lock().expired, 1);
    Ok(())
}

#[tokio::test]
async fn stage_lag_reports_the_raw_backlog() -> PlatformResult<()> {
    let catalog = Arc::new(FakeCatalog {
        metadata_files: Mutex::new(0),
        metadata_growth: 0,
        state: Mutex::new(FakeCatalogState {
            raw_plans: HashMap::from([(
                ArchiveKind::Events,
                FakeCatalog::raw_plan(
                    &["s3://archive/raw/events/a.parquet"],
                    Duration::from_secs(86_400),
                ),
            )]),
            ..FakeCatalogState::default()
        }),
        orphans: HashMap::new(),
        data_files: HashMap::new(),
    });
    let objects = FakeObjects::new();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let maintenance = executor(catalog.clone(), objects.clone(), leases.clone())?;

    maintenance
        .plan(MaintenanceStage::RawDeletion)
        .await
        .unwrap();

    assert_eq!(
        maintenance.stage_lag(MaintenanceStage::RawDeletion).await?,
        Duration::from_secs(86_400)
    );
    Ok(())
}
