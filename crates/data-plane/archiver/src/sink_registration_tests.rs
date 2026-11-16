use crate::sink::S3ParquetSink;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveRegistration, Bucket, CommitContext,
    ConflictDetails, IcebergDataFile, RegisterOutcome,
};
use platform_errors::PlatformResult;
use platform_lease::InMemoryLeaseStore;
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
    fn with_outcomes(
        live: impl IntoIterator<Item = String>,
        outcomes: impl IntoIterator<Item = RegisterOutcome>,
    ) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            live: Mutex::new(live.into_iter().collect()),
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
        if matches!(outcome, RegisterOutcome::Applied) {
            self.live.lock().unwrap().extend(outputs);
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

struct Fixture;

impl Fixture {
    fn sink(catalog: Arc<FakeCatalog>) -> S3ParquetSink {
        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "flush-test"))
            .build();
        S3ParquetSink {
            s3_client: S3Client::from_conf(config),
            catalog,
            leases: Arc::new(InMemoryLeaseStore::new()),
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

    fn files() -> Vec<IcebergDataFile> {
        vec![
            IcebergDataFile::new("s3://archive/raw/a.parquet", 10, 1),
            IcebergDataFile::new("s3://archive/raw/b.parquet", 10, 1),
        ]
    }

    fn conflict(live_outputs: &[&str]) -> RegisterOutcome {
        RegisterOutcome::Conflict(ConflictDetails {
            claimed_sources: Vec::new(),
            live_outputs: live_outputs.iter().map(|path| path.to_string()).collect(),
        })
    }
}

#[tokio::test]
async fn a_partial_flush_conflict_registers_only_the_remainder() {
    let live = ["s3://archive/raw/a.parquet".to_string()];
    let catalog = Arc::new(FakeCatalog::with_outcomes(
        live,
        [
            Fixture::conflict(&["s3://archive/raw/a.parquet"]),
            RegisterOutcome::Applied,
        ],
    ));
    let sink = Fixture::sink(catalog.clone());

    sink.register_flush(ArchiveIcebergTable::RawEvents, Fixture::files())
        .await
        .unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 2);
    let batches = catalog.batches.lock().unwrap();
    assert_eq!(batches[0].len(), 2);
    assert_eq!(batches[1], vec!["s3://archive/raw/b.parquet".to_string()]);
}

#[tokio::test]
async fn a_flush_conflict_that_is_already_live_succeeds_without_retrying() {
    let live = [
        "s3://archive/raw/a.parquet".to_string(),
        "s3://archive/raw/b.parquet".to_string(),
    ];
    let catalog = Arc::new(FakeCatalog::with_outcomes(
        live,
        [Fixture::conflict(&[
            "s3://archive/raw/a.parquet",
            "s3://archive/raw/b.parquet",
        ])],
    ));
    let sink = Fixture::sink(catalog.clone());

    sink.register_flush(ArchiveIcebergTable::RawEvents, Fixture::files())
        .await
        .unwrap();

    assert_eq!(catalog.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_flush_that_keeps_conflicting_fails_after_the_retry_limit() {
    let catalog = Arc::new(FakeCatalog::with_outcomes(
        [],
        [
            Fixture::conflict(&[]),
            Fixture::conflict(&[]),
            Fixture::conflict(&[]),
            Fixture::conflict(&[]),
            Fixture::conflict(&[]),
        ],
    ));
    let sink = Fixture::sink(catalog.clone());

    let error = sink
        .register_flush(ArchiveIcebergTable::RawEvents, Fixture::files())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("remained conflicted"));
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 4);
}
