use crate::object_lister::ObjectLister;
use crate::source::{ArchiveBatchSource, S3ArchiveSource};
use crate::source_inventory::INVENTORY_RECONCILE_POLLS;
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchivePath, ArchiveRegistration, Bucket,
    CommitContext, ObjectKey, RegisterOutcome, RoutingBucket, S3Object, S3ObjectPage, SourceBatch,
};
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

#[derive(Default)]
struct FakeLister {
    objects: Mutex<BTreeMap<String, S3Object>>,
    omitted: Mutex<HashSet<String>>,
}

impl FakeLister {
    fn insert(&self, object: S3Object) {
        self.objects
            .lock()
            .insert(object.key.as_str().to_string(), object);
    }

    fn omit(&self, key: &str) {
        self.omitted.lock().insert(key.to_string());
    }

    fn restore(&self, key: &str) {
        self.omitted.lock().remove(key);
    }
}

#[async_trait]
impl ObjectLister for FakeLister {
    async fn list_page(
        &self,
        _bucket: &Bucket,
        prefix: &ObjectKey,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: i32,
    ) -> PlatformResult<S3ObjectPage> {
        let after = continuation_token.or(start_after).unwrap_or("");
        let omitted = self.omitted.lock();
        let objects = self
            .objects
            .lock()
            .range(after.to_string()..)
            .filter(|(key, _)| {
                key.starts_with(prefix.as_str()) && key.as_str() > after && !omitted.contains(*key)
            })
            .map(|(_, object)| object.clone())
            .collect::<Vec<_>>();
        let limit = usize::try_from(max_keys.clamp(1, 1_000)).unwrap_or(1_000);
        let has_more = objects.len() > limit;
        let objects = objects.into_iter().take(limit).collect::<Vec<_>>();
        let next_continuation_token = has_more
            .then(|| objects.last().map(|object| object.key.as_str().to_string()))
            .flatten();
        Ok(S3ObjectPage {
            objects,
            next_continuation_token,
        })
    }
}

#[derive(Default)]
struct FakeCatalog {
    raw_live: Mutex<HashSet<String>>,
    compacted_claimed: Mutex<HashSet<String>>,
}

impl FakeCatalog {
    fn live(&self, uri: &str) {
        self.raw_live.lock().insert(uri.to_string());
    }

    fn claim(&self, uri: &str) {
        self.compacted_claimed.lock().insert(uri.to_string());
    }
}

#[async_trait]
impl ArchiveCatalog for FakeCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        _registration: &ArchiveRegistration,
        _context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        panic!("inventory polling must not register files")
    }

    async fn registered_data_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(self.raw_live.lock().clone())
    }

    async fn processed_source_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(self.compacted_claimed.lock().clone())
    }
}

fn object(key: &str, modified: i64) -> S3Object {
    S3Object {
        key: ObjectKey::new(key),
        last_modified_millis: Some(modified),
        size: 1,
    }
}

fn batches(batches: Vec<SourceBatch>) -> Vec<Vec<String>> {
    batches
        .into_iter()
        .map(|batch| batch.files)
        .collect::<Vec<_>>()
}

#[tokio::test]
async fn an_inventory_gap_is_reconciled_without_skipping_or_duplicating_a_source() {
    let bucket = Bucket::new("archive");
    let first = ArchivePath::raw_key(
        "raw",
        ArchiveKind::Events,
        TimestampMilliseconds(0),
        RoutingBucket::from_index(0).unwrap(),
        "a.parquet",
    )
    .unwrap();
    let second = ArchivePath::raw_key(
        "raw",
        ArchiveKind::Events,
        TimestampMilliseconds(0),
        RoutingBucket::from_index(0).unwrap(),
        "b.parquet",
    )
    .unwrap();
    let first_uri = format!("s3://{bucket}/{first}");
    let second_uri = format!("s3://{bucket}/{second}");
    let modified = TimestampMilliseconds::now().0;
    let lister = Arc::new(FakeLister::default());
    lister.insert(object(&first, modified));
    let catalog = Arc::new(FakeCatalog::default());
    catalog.live(&first_uri);
    let source = S3ArchiveSource::new(catalog.clone(), lister.clone(), bucket, "raw".into(), 1, 0);

    let initial = source.pending_batches().await.unwrap();
    assert_eq!(batches(initial), vec![vec![first.clone()]]);

    catalog.claim(&first_uri);
    let completed = source.pending_batches().await.unwrap();
    assert!(completed.is_empty());

    lister.insert(object(&second, modified));
    lister.omit(&second);
    let omitted = source.pending_batches().await.unwrap();
    assert!(omitted.is_empty());

    catalog.live(&second_uri);
    lister.restore(&second);
    source.inventory.lock().await.polls = INVENTORY_RECONCILE_POLLS;
    let reconciled = source.pending_batches().await.unwrap();
    assert_eq!(batches(reconciled), vec![vec![second.clone()]]);

    catalog.claim(&second_uri);
    let finished = source.pending_batches().await.unwrap();
    assert!(finished.is_empty());
}
