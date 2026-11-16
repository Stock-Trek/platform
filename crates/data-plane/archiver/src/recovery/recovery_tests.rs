use super::S3ParquetSink;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveRegistration, Bucket, CommitContext,
    ConflictDetails, IcebergDataFile, ObjectKey, ObjectStore, ParquetRowCounter, RegisterOutcome,
    S3Object, S3ObjectPage,
};
use platform_errors::PlatformResult;
use platform_lease::{InMemoryLeaseStore, LeaseGuard, LeaseRequest, LeaseScope, LeaseStore};
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
        _context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut outputs = registration
            .expected_outputs()
            .into_iter()
            .collect::<Vec<_>>();
        outputs.sort();
        self.batches.lock().unwrap().push(outputs.clone());
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
        if matches!(outcome, RegisterOutcome::Applied) {
            self.live.lock().unwrap().extend(outputs.iter().cloned());
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

struct FakeObjectStore {
    pages: Mutex<VecDeque<Vec<S3Object>>>,
    observed_continuations: Mutex<Vec<Option<String>>>,
    calls: AtomicUsize,
    delayed_call: usize,
    delay: Duration,
}

impl FakeObjectStore {
    fn with_pages(pages: Vec<Vec<S3Object>>) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            observed_continuations: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            delayed_call: 0,
            delay: Duration::ZERO,
        }
    }

    fn with_pages_and_delay(pages: Vec<Vec<S3Object>>, call: usize, delay: Duration) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            observed_continuations: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            delayed_call: call,
            delay,
        }
    }

    fn observed_continuations(&self) -> Vec<Option<String>> {
        self.observed_continuations.lock().unwrap().clone()
    }
}

#[async_trait]
impl ObjectStore for FakeObjectStore {
    async fn list_page(
        &self,
        _bucket: &Bucket,
        _prefix: &ObjectKey,
        continuation_token: Option<&str>,
        _max_keys: i32,
    ) -> PlatformResult<S3ObjectPage> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.observed_continuations
            .lock()
            .unwrap()
            .push(continuation_token.map(str::to_string));
        if call == self.delayed_call {
            tokio::time::sleep(self.delay).await;
        }
        let objects = self.pages.lock().unwrap().pop_front().unwrap_or_default();
        let next_continuation_token = if self.pages.lock().unwrap().is_empty() {
            None
        } else {
            Some(format!("page-{call}"))
        };
        Ok(S3ObjectPage {
            objects,
            next_continuation_token,
        })
    }

    async fn delete(&self, _bucket: &Bucket, _key: &ObjectKey) -> PlatformResult<()> {
        Ok(())
    }
}

struct FakeRowCounter {
    count: i64,
}

#[async_trait]
impl ParquetRowCounter for FakeRowCounter {
    async fn row_count(
        &self,
        _bucket: &Bucket,
        _key: &ObjectKey,
        _file_size: u64,
    ) -> PlatformResult<i64> {
        Ok(self.count)
    }
}

struct RecoveryFixture;

impl RecoveryFixture {
    fn sink(catalog: Arc<FakeCatalog>, leases: Arc<InMemoryLeaseStore>) -> S3ParquetSink {
        Self::sink_with(
            catalog,
            leases,
            Arc::new(FakeObjectStore::with_pages(Vec::new())),
        )
    }

    fn sink_with(
        catalog: Arc<FakeCatalog>,
        leases: Arc<InMemoryLeaseStore>,
        objects: Arc<FakeObjectStore>,
    ) -> S3ParquetSink {
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
        let client = S3Client::from_conf(config);
        S3ParquetSink {
            s3_client: client,
            objects,
            row_counter: Arc::new(FakeRowCounter { count: 1 }),
            catalog,
            leases,
            bucket: Bucket::new("archive".to_string()),
            prefix: "raw".to_string(),
            bucket_millis: 3_600_000,
            lease_ttl: Duration::from_secs(30),
            max_work: Duration::from_secs(20),
            lease_initial_token: 1,
            lease_clock_skew: Duration::ZERO,
            recovery_page_files: 1_000,
            recovery_page_bytes: 64 * 1024 * 1024,
        }
    }

    fn object(name: &str) -> S3Object {
        S3Object {
            key: ObjectKey::new(format!(
                "raw/events/bucket=00000/year=2024/month=01/day=01/hour=00/{name}.parquet"
            )),
            last_modified_millis: Some(0),
            size: 10,
        }
    }

    fn location(object: &S3Object) -> String {
        format!("s3://archive/{}", object.key)
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

#[tokio::test]
async fn recovery_aborts_before_registering_after_its_soft_deadline() {
    let catalog = Arc::new(FakeCatalog::with_outcomes([RegisterOutcome::Applied]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let sink = RecoveryFixture::sink(catalog.clone(), leases.clone());
    let mut guard = LeaseGuard::acquire(
        leases,
        LeaseRequest::new(
            LeaseScope::new("archive-recovery:raw_events"),
            "recovery-owner",
            Duration::from_millis(60),
        ),
        Duration::from_millis(20),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let result = sink
        .register_pending(
            ArchiveIcebergTable::RawEvents,
            RecoveryFixture::pending(),
            &mut guard,
        )
        .await;

    assert!(result.is_err());
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn recovery_scopes_include_the_bucket_and_prefix_identity() {
    let leases = Arc::new(InMemoryLeaseStore::new());
    let mut sink = RecoveryFixture::sink(Arc::new(FakeCatalog::with_outcomes([])), leases);
    let original = sink.recovery_scope(ArchiveIcebergTable::RawEvents);
    sink.bucket = Bucket::new("other-archive".to_string());

    assert_ne!(
        original.as_str(),
        sink.recovery_scope(ArchiveIcebergTable::RawEvents).as_str()
    );

    let bucket_scope = sink.recovery_scope(ArchiveIcebergTable::RawEvents);
    sink.prefix = "other-raw".to_string();

    assert_ne!(
        bucket_scope.as_str(),
        sink.recovery_scope(ArchiveIcebergTable::RawEvents).as_str()
    );
}

#[tokio::test]
async fn recovery_pages_respect_both_file_and_byte_boundaries() {
    let catalog = Arc::new(FakeCatalog::with_outcomes([]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let mut sink = RecoveryFixture::sink(catalog.clone(), leases.clone());
    sink.recovery_page_files = 1;
    let mut guard = RecoveryFixture::guard(leases.clone()).await;
    let pending = vec![
        IcebergDataFile::new("s3://archive/raw/a.parquet", 10, 1),
        IcebergDataFile::new("s3://archive/raw/b.parquet", 10, 1),
        IcebergDataFile::new("s3://archive/raw/c.parquet", 10, 1),
    ];

    sink.register_pending_pages(ArchiveIcebergTable::RawEvents, pending, &mut guard)
        .await
        .unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 3);
    {
        let batches = catalog.batches.lock().unwrap();
        assert!(batches.iter().all(|batch| batch.len() == 1));
    }

    let catalog = Arc::new(FakeCatalog::with_outcomes([]));
    let mut sink = RecoveryFixture::sink(catalog.clone(), leases);
    sink.recovery_page_files = 100;
    sink.recovery_page_bytes = 15;
    sink.register_pending_pages(
        ArchiveIcebergTable::RawEvents,
        RecoveryFixture::pending(),
        &mut guard,
    )
    .await
    .unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn recovery_registers_each_object_once_across_listing_pages() {
    let objects = Arc::new(FakeObjectStore::with_pages(vec![
        vec![RecoveryFixture::object("a")],
        vec![RecoveryFixture::object("b")],
        vec![RecoveryFixture::object("c")],
    ]));
    let catalog = Arc::new(FakeCatalog::with_outcomes([]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let mut sink = RecoveryFixture::sink_with(catalog.clone(), leases, objects.clone());
    sink.recovery_page_files = 1;

    sink.recover_uncommitted().await.unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 3);
    let batches = catalog.batches.lock().unwrap();
    let mut registered = batches.iter().flatten().cloned().collect::<Vec<_>>();
    registered.sort();
    registered.dedup();
    assert_eq!(registered.len(), 3);
    assert_eq!(
        objects.observed_continuations(),
        vec![
            None,
            Some("page-1".to_string()),
            Some("page-2".to_string()),
            None
        ]
    );
}

#[tokio::test]
async fn recovery_registers_only_the_remainder_after_a_cross_page_conflict() {
    let objects = Arc::new(FakeObjectStore::with_pages(vec![
        vec![RecoveryFixture::object("a"), RecoveryFixture::object("b")],
        vec![RecoveryFixture::object("c")],
    ]));
    let first = RecoveryFixture::object("a");
    let catalog = Arc::new(FakeCatalog::with_outcomes([
        RegisterOutcome::Conflict(ConflictDetails {
            claimed_sources: Vec::new(),
            live_outputs: vec![RecoveryFixture::location(&first)],
        }),
        RegisterOutcome::Applied,
        RegisterOutcome::Applied,
    ]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let mut sink = RecoveryFixture::sink_with(catalog.clone(), leases, objects);
    sink.recovery_page_files = 2;

    sink.recover_uncommitted().await.unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 3);
    let batches = catalog.batches.lock().unwrap();
    assert_eq!(batches[0].len(), 2);
    assert_eq!(
        batches[1],
        vec![RecoveryFixture::location(&RecoveryFixture::object("b"))]
    );
    assert_eq!(
        batches[2],
        vec![RecoveryFixture::location(&RecoveryFixture::object("c"))]
    );
}

#[tokio::test]
async fn recovery_restart_skips_objects_that_are_already_live() {
    let first = RecoveryFixture::object("a");
    let objects = Arc::new(FakeObjectStore::with_pages(vec![
        vec![first.clone()],
        vec![RecoveryFixture::object("b")],
    ]));
    let catalog = Arc::new(FakeCatalog::with_outcomes([]));
    catalog
        .live
        .lock()
        .unwrap()
        .insert(RecoveryFixture::location(&first));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let mut sink = RecoveryFixture::sink_with(catalog.clone(), leases, objects);
    sink.recovery_page_files = 1;

    sink.recover_uncommitted().await.unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 1);
    let batches = catalog.batches.lock().unwrap();
    assert_eq!(
        batches[0],
        vec![RecoveryFixture::location(&RecoveryFixture::object("b"))]
    );
}

#[tokio::test]
async fn recovery_stops_between_pages_at_the_soft_deadline_and_releases_the_lease() {
    let objects = Arc::new(FakeObjectStore::with_pages_and_delay(
        vec![
            vec![RecoveryFixture::object("a")],
            vec![RecoveryFixture::object("b")],
        ],
        2,
        Duration::from_millis(50),
    ));
    let catalog = Arc::new(FakeCatalog::with_outcomes([]));
    let leases = Arc::new(InMemoryLeaseStore::new());
    let mut sink = RecoveryFixture::sink_with(catalog.clone(), leases.clone(), objects);
    sink.recovery_page_files = 1;
    sink.max_work = Duration::from_millis(30);

    let result = sink.recover_uncommitted().await;

    assert!(result.is_err());
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 1);
    let next = leases
        .acquire(&LeaseRequest::new(
            sink.recovery_scope(ArchiveIcebergTable::RawEvents),
            "next-recovery",
            Duration::from_secs(30),
        ))
        .await
        .unwrap();
    assert!(next.is_some());
}
