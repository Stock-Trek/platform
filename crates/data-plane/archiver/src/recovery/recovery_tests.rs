use super::S3ParquetSink;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveRegistration, Bucket, ConflictDetails,
    IcebergDataFile, RegisterOutcome,
};
use platform_errors::PlatformResult;
use platform_lease::{InMemoryLeaseStore, LeaseGuard, LeaseRequest, LeaseScope};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct FakeCatalog {
    outcomes: Mutex<VecDeque<RegisterOutcome>>,
    live: Mutex<HashSet<String>>,
    batches: Mutex<Vec<Vec<String>>>,
    calls: AtomicUsize,
}

impl FakeCatalog {
    fn with_outcomes(outcomes: impl IntoIterator<Item = RegisterOutcome>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            live: Mutex::new(HashSet::new()),
            batches: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl ArchiveCatalog for FakeCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<RegisterOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut outputs = registration
            .expected_outputs()
            .into_iter()
            .collect::<Vec<_>>();
        outputs.sort();
        self.batches.lock().unwrap().push(outputs);
        let outcome = self
            .outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(RegisterOutcome::Applied);
        if let RegisterOutcome::Conflict(conflict) = &outcome {
            self.live
                .lock()
                .unwrap()
                .extend(conflict.live_outputs.iter().cloned());
        }
        Ok(outcome)
    }

    async fn registered_data_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(self.live.lock().unwrap().clone())
    }

    async fn processed_source_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(HashSet::new())
    }
}

struct RecoveryFixture;

impl RecoveryFixture {
    fn sink(catalog: Arc<FakeCatalog>, leases: Arc<InMemoryLeaseStore>) -> S3ParquetSink {
        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test",
                "test",
                None,
                None,
                "recovery-test",
            ))
            .build();
        S3ParquetSink {
            s3_client: S3Client::from_conf(config),
            catalog,
            leases,
            bucket: Bucket::new("archive".to_string()),
            prefix: "raw".to_string(),
            bucket_millis: 3_600_000,
            lease_ttl: Duration::from_secs(30),
            max_work: Duration::from_secs(20),
            lease_initial_token: 1,
        }
    }

    async fn guard(leases: Arc<InMemoryLeaseStore>) -> LeaseGuard {
        LeaseGuard::acquire(
            leases,
            LeaseRequest::new(
                LeaseScope::new("archive-recovery:raw_events"),
                "recovery-owner",
                Duration::from_secs(30),
            ),
            Duration::from_secs(20),
        )
        .await
        .unwrap()
        .unwrap()
    }

    fn pending() -> Vec<IcebergDataFile> {
        vec![
            IcebergDataFile::new("s3://archive/raw/a.parquet", 10, 1),
            IcebergDataFile::new("s3://archive/raw/b.parquet", 10, 1),
        ]
    }
}

#[tokio::test]
async fn a_recovery_conflict_retries_only_the_uncommitted_files() {
    let catalog = Arc::new(FakeCatalog::with_outcomes([
        RegisterOutcome::Conflict(ConflictDetails {
            claimed_sources: Vec::new(),
            live_outputs: vec!["s3://archive/raw/a.parquet".to_string()],
        }),
        RegisterOutcome::Applied,
    ]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let sink = RecoveryFixture::sink(catalog.clone(), leases.clone());
    let mut guard = RecoveryFixture::guard(leases).await;

    sink.register_pending(
        ArchiveIcebergTable::RawEvents,
        RecoveryFixture::pending(),
        &mut guard,
    )
    .await
    .unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 2);
    let batches = catalog.batches.lock().unwrap();
    assert_eq!(batches[0].len(), 2);
    assert_eq!(batches[1], vec!["s3://archive/raw/b.parquet".to_string()]);
}

#[tokio::test]
async fn recovery_reuses_an_already_applied_registration() {
    let catalog = Arc::new(FakeCatalog::with_outcomes([
        RegisterOutcome::AlreadyApplied,
        RegisterOutcome::Applied,
    ]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let sink = RecoveryFixture::sink(catalog.clone(), leases.clone());
    let mut guard = RecoveryFixture::guard(leases).await;

    sink.register_pending(
        ArchiveIcebergTable::RawEvents,
        RecoveryFixture::pending(),
        &mut guard,
    )
    .await
    .unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_reports_files_that_never_leave_conflict() {
    let conflict = RegisterOutcome::Conflict(ConflictDetails {
        claimed_sources: Vec::new(),
        live_outputs: Vec::new(),
    });
    let catalog = Arc::new(FakeCatalog::with_outcomes(vec![conflict; 4]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let sink = RecoveryFixture::sink(catalog.clone(), leases.clone());
    let mut guard = RecoveryFixture::guard(leases).await;

    let result = sink
        .register_pending(
            ArchiveIcebergTable::RawEvents,
            RecoveryFixture::pending(),
            &mut guard,
        )
        .await;

    assert!(result.is_err());
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 4);
}
