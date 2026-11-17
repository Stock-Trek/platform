use super::Compactor;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use parking_lot::Mutex;
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchiveRegistration, CommitContext,
    ConflictDetails, IcebergDataFile, MaintenancePartition, RegisterOutcome, SourceBatch,
};
use platform_errors::PlatformResult;
use platform_lease::{InMemoryLeaseStore, LeaseGuard, LeaseRequest, LeaseScope, LeaseStore};
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

struct FakeCatalog {
    claimed: HashSet<String>,
    outcome: Option<RegisterOutcome>,
    registrations: Mutex<Vec<ArchiveRegistration>>,
}

#[async_trait]
impl ArchiveCatalog for FakeCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
        _context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        let Some(outcome) = &self.outcome else {
            panic!("registration must not be attempted in this coordination test")
        };
        self.registrations.lock().push(registration.clone());
        Ok(outcome.clone())
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
        Ok(self.claimed.clone())
    }
}

impl Compactor {
    fn for_coordination(catalog: Arc<dyn ArchiveCatalog>, leases: Arc<dyn LeaseStore>) -> Self {
        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test",
                "test",
                None,
                None,
                "coordination-test",
            ))
            .build();
        let client = S3Client::from_conf(config);
        Self {
            client,
            catalog,
            bucket: "archive".to_string(),
            output_prefix: "compacted".to_string(),
            spill_records: 1000,
            leases,
            lease_ttl_events: Duration::from_secs(30),
            lease_ttl_candles: Duration::from_secs(30),
            max_work: Duration::from_secs(20),
            lease_initial_token: 1,
            lease_clock_skew: Duration::ZERO,
        }
    }
}

impl FakeCatalog {
    fn batch() -> SourceBatch {
        SourceBatch {
            kind: ArchiveKind::Events,
            prefix: "raw/kind=events".to_string(),
            bucket_start: TimestampMilliseconds(0),
            files: vec!["raw/kind=events/a.parquet".to_string()],
        }
    }

    fn with_claimed(batch: &SourceBatch) -> Self {
        Self {
            claimed: batch
                .files
                .iter()
                .map(|file| format!("s3://archive/{file}"))
                .collect(),
            outcome: None,
            registrations: Mutex::new(Vec::new()),
        }
    }

    fn with_outcome(outcome: RegisterOutcome) -> Self {
        Self {
            claimed: HashSet::new(),
            outcome: Some(outcome),
            registrations: Mutex::new(Vec::new()),
        }
    }
}

#[tokio::test]
async fn another_compactor_holding_the_scope_makes_this_compactor_skip() {
    let batch = FakeCatalog::batch();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let compactor = Compactor::for_coordination(
        Arc::new(FakeCatalog {
            claimed: HashSet::new(),
            outcome: None,
            registrations: Mutex::new(Vec::new()),
        }),
        leases.clone(),
    );
    let held = leases
        .acquire(&LeaseRequest::new(
            LeaseScope::new(compactor.lease_scope(&batch)),
            "another-compactor",
            Duration::from_secs(30),
        ))
        .await
        .unwrap()
        .unwrap();

    let stats = compactor.compact(&batch).await.unwrap();

    assert!(stats.skipped);
    assert!(leases.is_current(&held).await.unwrap());
}

#[tokio::test]
async fn a_compactor_releases_the_scope_after_an_already_claimed_skip() {
    let batch = FakeCatalog::batch();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let compactor =
        Compactor::for_coordination(Arc::new(FakeCatalog::with_claimed(&batch)), leases.clone());
    let scope = LeaseScope::new(compactor.lease_scope(&batch));

    let stats = compactor.compact(&batch).await.unwrap();

    assert!(stats.skipped);
    assert!(leases.current(&scope).await.unwrap().is_none());
}

struct SlowCatalog;

#[async_trait]
impl ArchiveCatalog for SlowCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        _registration: &ArchiveRegistration,
        _context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        panic!("a timed-out compaction must not register files")
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
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(HashSet::new())
    }
}

#[tokio::test]
async fn a_compactor_aborts_and_releases_the_scope_after_its_soft_deadline() {
    let batch = FakeCatalog::batch();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let mut compactor = Compactor::for_coordination(Arc::new(SlowCatalog), leases.clone());
    compactor.max_work = Duration::from_millis(20);
    let scope = LeaseScope::new(compactor.lease_scope(&batch));

    let result = compactor.compact(&batch).await;

    assert!(result.is_err());
    assert!(leases.current(&scope).await.unwrap().is_none());
}

#[test]
fn concurrent_hourly_prefixes_use_distinct_compaction_scopes() {
    let mut first = FakeCatalog::batch();
    let mut second = FakeCatalog::batch();
    second.prefix = "raw/kind=events/hour=2024-01-01T00".to_string();
    first.prefix = "raw/kind=events/hour=2024-01-01T01".to_string();

    let compactor = Compactor::for_coordination(
        Arc::new(FakeCatalog::with_claimed(&first)),
        Arc::new(InMemoryLeaseStore::new()),
    );

    assert_eq!(
        compactor.lease_scope(&first),
        compactor.lease_scope(&first.clone())
    );
    assert_ne!(
        compactor.lease_scope(&first),
        compactor.lease_scope(&second)
    );
}

#[tokio::test]
async fn a_compaction_conflict_defers_the_source_claim_without_failing() {
    let batch = FakeCatalog::batch();
    let catalog = Arc::new(FakeCatalog::with_outcome(RegisterOutcome::Conflict(
        ConflictDetails {
            claimed_sources: Vec::new(),
            live_outputs: Vec::new(),
        },
    )));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let compactor = Compactor::for_coordination(catalog.clone(), leases.clone());
    let guard = LeaseGuard::acquire(
        leases.clone(),
        LeaseRequest::new(
            LeaseScope::new(compactor.lease_scope(&batch)),
            "test-compactor",
            Duration::from_secs(30),
        ),
        Duration::from_secs(20),
    )
    .await
    .unwrap()
    .unwrap();
    let registration = ArchiveRegistration::new(
        vec![IcebergDataFile::new(
            "s3://archive/compacted/kind=events/part.parquet",
            10,
            1,
        )],
        Some(batch.files.clone()),
    )
    .unwrap();
    let context = CommitContext::default().with_lease(
        guard.lease().scope.as_str(),
        &guard.lease().owner_id,
        guard.lease().fencing_token,
    );

    let skipped = compactor
        .register_compaction(
            &batch,
            ArchiveIcebergTable::compacted(batch.kind),
            &registration,
            &context,
            &guard,
        )
        .await
        .unwrap();

    assert!(skipped);
    {
        let registrations = catalog.registrations.lock();
        assert_eq!(registrations.len(), 1);
        assert_eq!(
            registrations[0].source_files().unwrap(),
            batch.files.as_slice()
        );
    }
    guard.release().await.unwrap();
}

#[tokio::test]
async fn a_successful_compaction_registration_is_not_reported_as_skipped() {
    let batch = FakeCatalog::batch();
    let catalog = Arc::new(FakeCatalog::with_outcome(RegisterOutcome::Applied));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let compactor = Compactor::for_coordination(catalog.clone(), leases.clone());
    let guard = LeaseGuard::acquire(
        leases.clone(),
        LeaseRequest::new(
            LeaseScope::new(compactor.lease_scope(&batch)),
            "test-compactor",
            Duration::from_secs(30),
        ),
        Duration::from_secs(20),
    )
    .await
    .unwrap()
    .unwrap();
    let registration = ArchiveRegistration::new(
        vec![IcebergDataFile::new(
            "s3://archive/compacted/kind=events/part.parquet",
            10,
            1,
        )],
        Some(batch.files.clone()),
    )
    .unwrap();
    let context = CommitContext::default();

    let skipped = compactor
        .register_compaction(
            &batch,
            ArchiveIcebergTable::compacted(batch.kind),
            &registration,
            &context,
            &guard,
        )
        .await
        .unwrap();

    assert!(!skipped);
    assert_eq!(catalog.registrations.lock().len(), 1);
    guard.release().await.unwrap();
}

#[test]
fn compaction_scopes_include_the_bucket_and_output_prefix_identity() {
    let batch = FakeCatalog::batch();
    let mut compactor = Compactor::for_coordination(
        Arc::new(FakeCatalog::with_claimed(&batch)),
        Arc::new(InMemoryLeaseStore::new()),
    );
    let original = compactor.lease_scope(&batch);
    compactor.bucket = "other-archive".to_string();

    assert_ne!(original, compactor.lease_scope(&batch));

    let bucket_scope = compactor.lease_scope(&batch);
    compactor.output_prefix = "other-compacted".to_string();

    assert_ne!(bucket_scope, compactor.lease_scope(&batch));
}

#[tokio::test]
async fn maintenance_holding_the_partition_lease_makes_this_compactor_skip() {
    let batch = FakeCatalog::batch();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let compactor = Compactor::for_coordination(
        Arc::new(FakeCatalog {
            claimed: HashSet::new(),
            outcome: None,
            registrations: Mutex::new(Vec::new()),
        }),
        leases.clone(),
    );
    let partition = MaintenancePartition::for_batch(&batch);
    let held = leases
        .acquire(&LeaseRequest::new(
            partition.lease_scope(&compactor.bucket, &compactor.output_prefix),
            "archive-maintainer",
            Duration::from_secs(600),
        ))
        .await
        .unwrap()
        .unwrap();

    let stats = compactor.compact(&batch).await.unwrap();

    assert!(stats.skipped);
    assert!(leases.is_current(&held).await.unwrap());
}
