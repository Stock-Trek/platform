use super::Compactor;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchiveRegistration, CommitContext,
    RegisterOutcome, SourceBatch,
};
use platform_errors::PlatformResult;
use platform_lease::{InMemoryLeaseStore, LeaseRequest, LeaseScope, LeaseStore};
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

struct FakeCatalog {
    claimed: HashSet<String>,
}

#[async_trait]
impl ArchiveCatalog for FakeCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        _registration: &ArchiveRegistration,
        _context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        panic!("registration must not be attempted in this coordination test")
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
        Self {
            client: S3Client::from_conf(config),
            catalog,
            bucket: "archive".to_string(),
            output_prefix: "compacted".to_string(),
            spill_records: 1000,
            leases,
            lease_ttl_events: Duration::from_secs(30),
            lease_ttl_candles: Duration::from_secs(30),
            max_work: Duration::from_secs(20),
            lease_initial_token: 1,
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
        }
    }
}

#[tokio::test]
async fn another_compactor_holding_the_scope_makes_this_compactor_skip() {
    let batch = FakeCatalog::batch();
    let leases = Arc::new(InMemoryLeaseStore::new());
    let held = leases
        .acquire(&LeaseRequest::new(
            LeaseScope::new(Compactor::lease_scope(&batch)),
            "another-compactor",
            Duration::from_secs(30),
        ))
        .await
        .unwrap()
        .unwrap();
    let compactor = Compactor::for_coordination(
        Arc::new(FakeCatalog {
            claimed: HashSet::new(),
        }),
        leases.clone(),
    );

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
    let scope = LeaseScope::new(Compactor::lease_scope(&batch));

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
    let scope = LeaseScope::new(Compactor::lease_scope(&batch));

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

    assert_eq!(
        Compactor::lease_scope(&first),
        Compactor::lease_scope(&first.clone())
    );
    assert_ne!(
        Compactor::lease_scope(&first),
        Compactor::lease_scope(&second)
    );
}
