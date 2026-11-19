use super::claim_index::{ClaimIndex, INDEX_HEAD_DIGEST_KEY, INDEX_HEAD_KEY};
use super::claim_index_batch::ClaimBatch;
use super::claim_index_page::{ClaimIndexPage, IndexParent};
use super::snapshot::SnapshotCommit;
use crate::ArchiveKind;
use crate::iceberg::CommitContext;
use crate::iceberg::catalog::{IcebergCatalog, IcebergDataFile};
use crate::{Bucket, ObjectKey, ObjectStore, S3, S3Object, S3ObjectPage};
use iceberg::{
    Runtime, TableIdent,
    io::FileIO,
    spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SortOrder, Summary,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    },
    table::Table,
};
use parking_lot::Mutex;
use platform_types::TimestampMilliseconds;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

impl ClaimIndexPage {
    pub(super) async fn write_consolidated(
        table: &Table,
        sources: HashSet<String>,
        outputs: HashSet<String>,
        depth: u64,
    ) -> platform_errors::PlatformResult<HashMap<String, String>> {
        let batches = if sources.is_empty() && outputs.is_empty() {
            Vec::new()
        } else {
            vec![ClaimBatch::from_claims(sources, outputs)]
        };
        Self::write_consolidated_batches(table, batches, depth).await
    }
}

struct MetadataTable;

impl MetadataTable {
    fn with_batches(batches: &[&[IcebergDataFile]]) -> Table {
        Self::with_properties(batches, HashMap::new())
    }

    fn with_properties(
        batches: &[&[IcebergDataFile]],
        properties: HashMap<String, String>,
    ) -> Table {
        Self::with_properties_on(FileIO::new_with_memory(), batches, properties)
    }

    fn with_properties_on(
        file_io: FileIO,
        batches: &[&[IcebergDataFile]],
        properties: HashMap<String, String>,
    ) -> Table {
        let summaries = batches
            .iter()
            .map(|files| {
                (
                    *files,
                    IcebergCatalog::snapshot_properties(files, None).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        Self::with_summaries_on(file_io, &summaries, properties)
    }

    fn with_summaries(batches: &[(&[IcebergDataFile], HashMap<String, String>)]) -> Table {
        Self::with_summaries_on(FileIO::new_with_memory(), batches, HashMap::new())
    }

    fn with_summaries_on(
        file_io: FileIO,
        batches: &[(&[IcebergDataFile], HashMap<String, String>)],
        properties: HashMap<String, String>,
    ) -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields([Arc::new(NestedField::required(
                1,
                "record",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let mut builder = TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://archive/table".to_string(),
            FormatVersion::V2,
            properties,
        )
        .unwrap();

        for (index, (_, summary)) in batches.iter().enumerate() {
            let index = i64::try_from(index).unwrap();
            let snapshot = Snapshot::builder()
                .with_snapshot_id(index + 1)
                .with_sequence_number(index)
                .with_timestamp_ms(index)
                .with_manifest_list(format!("s3://archive/table/metadata/snap-{index}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: summary.clone(),
                })
                .build();
            builder = builder.add_snapshot(snapshot).unwrap();
        }

        Table::builder()
            .file_io(file_io)
            .metadata_location("s3://archive/table/metadata/v1.json")
            .metadata(builder.build().unwrap().metadata)
            .identifier(TableIdent::from_strs(["ns", "table"]).unwrap())
            .runtime(Runtime::current())
            .build()
            .unwrap()
    }
}

struct FakeObjectStore {
    pages: Mutex<VecDeque<Vec<S3Object>>>,
    observed_continuations: Mutex<Vec<Option<String>>>,
    deleted: Mutex<Vec<String>>,
}

impl FakeObjectStore {
    fn with_pages(pages: Vec<Vec<S3Object>>) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            observed_continuations: Mutex::new(Vec::new()),
            deleted: Mutex::new(Vec::new()),
        }
    }

    fn observed_continuations(&self) -> Vec<Option<String>> {
        self.observed_continuations.lock().clone()
    }

    fn deleted(&self) -> Vec<String> {
        self.deleted.lock().clone()
    }

    fn object(key: impl Into<String>, last_modified_millis: i64) -> S3Object {
        S3Object {
            key: ObjectKey::new(key),
            last_modified_millis: Some(last_modified_millis),
            size: 1,
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for FakeObjectStore {
    async fn list_page(
        &self,
        _bucket: &Bucket,
        _prefix: &ObjectKey,
        continuation_token: Option<&str>,
        _max_keys: i32,
    ) -> platform_errors::PlatformResult<S3ObjectPage> {
        self.observed_continuations
            .lock()
            .push(continuation_token.map(str::to_string));
        let objects = self.pages.lock().pop_front().unwrap_or_default();
        let next_continuation_token = if self.pages.lock().is_empty() {
            None
        } else {
            Some(format!("page-{}", self.observed_continuations.lock().len()))
        };
        Ok(S3ObjectPage {
            objects,
            next_continuation_token,
        })
    }

    async fn delete(
        &self,
        _bucket: &Bucket,
        key: &ObjectKey,
    ) -> platform_errors::PlatformResult<()> {
        self.deleted.lock().push(key.as_str().to_string());
        Ok(())
    }
}

#[test]
fn batch_fingerprints_are_stable_and_order_sensitive() {
    let first = IcebergDataFile::new("s3://archive/a.parquet", 1, 2);
    let second = IcebergDataFile::new("s3://archive/b.parquet", 3, 4);

    assert_eq!(
        IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()]),
        IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()])
    );
    assert_ne!(
        IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()]),
        IcebergCatalog::batch_fingerprint(&[second, first])
    );
}

#[tokio::test]
async fn source_claims_require_a_version_count_and_matching_digest() {
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];
    let sources = vec!["s3://archive/raw/source.parquet".to_string()];
    let properties = IcebergCatalog::snapshot_properties(&files, Some(&sources)).unwrap();
    let table = MetadataTable::with_properties(&[&files], HashMap::new());

    assert_eq!(
        properties.get("archive-claim-version"),
        Some(&"1".to_string())
    );
    assert_eq!(properties.get("source-claim-count"), Some(&"1".to_string()));
    assert_eq!(
        IcebergCatalog::source_files_from_metadata(&table, false)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn snapshot_properties_record_source_files_alongside_the_batch_fingerprint() {
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];
    let source_files = ["s3://archive/raw/source.parquet".to_string()];
    let properties = IcebergCatalog::snapshot_properties(&files, Some(&source_files)).unwrap();

    assert_eq!(
        properties.get(&format!(
            "archive-batch:{}",
            IcebergCatalog::batch_fingerprint(&files)
        )),
        Some(&"1".to_string())
    );
    assert_eq!(
        properties.get("source-file:s3://archive/raw/source.parquet"),
        Some(&"1".to_string())
    );
}

#[test]
fn source_batch_digests_are_full_sha256_and_reject_empty_sets() {
    let sources = vec!["s3://archive/raw/source.parquet".to_string()];

    assert_eq!(
        IcebergCatalog::source_batch_digest(&sources).unwrap().len(),
        64
    );
    assert!(IcebergCatalog::source_batch_digest(&[]).is_err());
}

#[tokio::test]
async fn snapshot_commit_requests_are_conditional_on_uuid_and_main() {
    let table = MetadataTable::with_batches(&[]);
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];
    let commit =
        SnapshotCommit::build_with_table_properties(&table, &files, HashMap::new(), HashMap::new())
            .await
            .unwrap();
    let request = commit.to_request(&TableIdent::from_strs(["ns", "table"]).unwrap());

    assert_eq!(request["requirements"][0]["type"], "assert-table-uuid");
    assert_eq!(
        request["requirements"][0]["uuid"],
        table.metadata().uuid().to_string()
    );
    assert_eq!(request["requirements"][1]["type"], "assert-ref-snapshot-id");
    assert_eq!(request["requirements"][1]["ref"], "main");
    assert_eq!(request["updates"][0]["action"], "add-snapshot");
    assert_eq!(request["updates"][1]["action"], "set-snapshot-ref");
    assert_eq!(request["updates"][1]["ref-name"], "main");
    assert_eq!(request["updates"][1]["type"], "branch");
    assert_eq!(request["updates"][1]["min-snapshots-to-keep"], i32::MAX);
    assert_eq!(request["updates"][1]["max-snapshot-age-ms"], i64::MAX);
}

#[tokio::test]
async fn snapshot_commit_manifests_live_in_the_metadata_directory() {
    let table = MetadataTable::with_batches(&[]);
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];
    let commit =
        SnapshotCommit::build_with_table_properties(&table, &files, HashMap::new(), HashMap::new())
            .await
            .unwrap();
    let request = commit.to_request(&TableIdent::from_strs(["ns", "table"]).unwrap());
    let manifest_list = request["updates"][0]["snapshot"]["manifest-list"]
        .as_str()
        .unwrap();

    assert!(manifest_list.starts_with("s3://archive/table/metadata/snap-"));
    assert!(!manifest_list.contains("metadata.json"));
}

#[test]
fn empty_source_batches_cannot_produce_a_snapshot_property() {
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];

    assert!(IcebergCatalog::snapshot_properties(&files, Some(&[])).is_err());
}

#[tokio::test]
async fn missing_required_properties_include_retention_and_commit_retries() {
    let table = MetadataTable::with_properties(
        &[],
        HashMap::from([(
            "history.expire.min-snapshots-to-keep".to_string(),
            i32::MAX.to_string(),
        )]),
    );

    let missing = IcebergCatalog::missing_required_properties(&table);

    assert!(!missing.contains_key("history.expire.min-snapshots-to-keep"));
    assert_eq!(
        missing.get("history.expire.max-snapshot-age-ms"),
        Some(&i64::MAX.to_string())
    );
    assert_eq!(
        missing.get("commit.retry.num-retries"),
        Some(&"0".to_string())
    );
}

#[tokio::test]
async fn a_commit_asserts_the_table_uuid_it_was_built_for() {
    let first = MetadataTable::with_batches(&[]);
    let second = MetadataTable::with_batches(&[]);
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];

    let request =
        SnapshotCommit::build_with_table_properties(&first, &files, HashMap::new(), HashMap::new())
            .await
            .unwrap()
            .to_request(&TableIdent::from_strs(["ns", "table"]).unwrap());

    assert_eq!(
        request["requirements"][0]["uuid"],
        first.metadata().uuid().to_string()
    );
    assert_ne!(
        request["requirements"][0]["uuid"],
        second.metadata().uuid().to_string()
    );
}

#[tokio::test]
async fn table_validation_rejects_a_schema_outside_the_archive_contract() {
    let table = MetadataTable::with_batches(&[]);

    assert!(
        IcebergCatalog::validate_table(
            &table,
            crate::iceberg::ArchiveIcebergTable::compacted(ArchiveKind::Events)
        )
        .is_err()
    );
}

#[tokio::test]
async fn claim_index_pages_round_trip_and_verify_their_digest() {
    let table = MetadataTable::with_batches(&[]);
    let page = ClaimIndexPage::from_claims(
        None,
        HashSet::from(["s3://archive/raw/a.parquet".to_string()]),
        HashSet::from(["s3://archive/out/a.parquet".to_string()]),
    );
    let properties = page.write(&table, 1).await.unwrap();
    let parent = IndexParent {
        location: properties.get(INDEX_HEAD_KEY).unwrap().clone(),
        digest: properties.get(INDEX_HEAD_DIGEST_KEY).unwrap().clone(),
    };

    let loaded = ClaimIndexPage::read(&table, &parent, None).await.unwrap();
    assert_eq!(loaded.sources, ["s3://archive/raw/a.parquet"]);
    assert_eq!(loaded.outputs, ["s3://archive/out/a.parquet"]);

    let corrupt = IndexParent {
        location: parent.location,
        digest: "0".repeat(64),
    };
    assert!(ClaimIndexPage::read(&table, &corrupt, None).await.is_err());
}

#[tokio::test]
async fn retention_becomes_finite_once_the_claim_index_is_authoritative() {
    let table = MetadataTable::with_properties(
        &[],
        HashMap::from([(INDEX_HEAD_KEY.to_string(), "s3://archive/head".to_string())]),
    );
    let missing = IcebergCatalog::missing_required_properties(&table);

    assert_eq!(
        missing.get("history.expire.min-snapshots-to-keep"),
        Some(&"10".to_string())
    );
    assert_eq!(
        missing.get("history.expire.max-snapshot-age-ms"),
        Some(&(30 * 24 * 60 * 60 * 1_000_i64).to_string())
    );
}

#[tokio::test]
async fn snapshot_commits_serialize_claim_properties_in_the_snapshot_summary() {
    let table = MetadataTable::with_batches(&[]);
    let files = [IcebergDataFile::new("s3://archive/out/a.parquet", 1, 2)];
    let sources = ["s3://archive/raw/source.parquet".to_string()];
    let properties = IcebergCatalog::snapshot_properties(&files, Some(&sources)).unwrap();
    let request =
        SnapshotCommit::build_with_table_properties(&table, &files, properties, HashMap::new())
            .await
            .unwrap()
            .to_request(&TableIdent::from_strs(["ns", "table"]).unwrap());
    let summary = &request["updates"][0]["snapshot"]["summary"];

    assert_eq!(
        summary[&format!(
            "archive-batch:{}",
            IcebergCatalog::batch_fingerprint(&files)
        )],
        "1"
    );
    assert_eq!(summary["source-file:s3://archive/raw/source.parquet"], "1");
    assert_eq!(summary["source-claim-count"], "1");
    assert_eq!(summary["archive-claim-version"], "1");
}

#[tokio::test]
async fn consolidated_claim_indexes_load_and_look_up_block_entries() {
    let table = MetadataTable::with_batches(&[]);
    let file_io = table.file_io().clone();
    let sources = (0..64)
        .map(|index| format!("s3://archive/raw/{index}.parquet"))
        .collect::<HashSet<_>>();
    let outputs = (0..64)
        .map(|index| format!("s3://archive/out/{index}.parquet"))
        .collect::<HashSet<_>>();
    let properties =
        ClaimIndexPage::write_consolidated(&table, sources.clone(), outputs.clone(), 1)
            .await
            .unwrap();
    let table = MetadataTable::with_properties_on(file_io, &[], properties);

    let loaded = ClaimIndex::load(&table).await.unwrap();
    assert_eq!(loaded.sources, sources);
    assert_eq!(loaded.outputs, outputs);

    let wanted_sources = vec!["s3://archive/raw/7.parquet".to_string()];
    let wanted_outputs = vec!["s3://archive/out/42.parquet".to_string()];
    let found = ClaimIndex::find_cached(&table, None, &wanted_sources, &wanted_outputs)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.sources, HashSet::from_iter(wanted_sources));
    assert_eq!(found.outputs, HashSet::from_iter(wanted_outputs));
}

#[tokio::test]
async fn claim_index_deltas_chain_to_the_consolidated_parent() {
    let table = MetadataTable::with_batches(&[]);
    let file_io = table.file_io().clone();
    let first = ClaimIndexPage::write_consolidated(
        &table,
        HashSet::from(["s3://archive/raw/a.parquet".to_string()]),
        HashSet::from(["s3://archive/out/a.parquet".to_string()]),
        1,
    )
    .await
    .unwrap();
    let table = MetadataTable::with_properties_on(file_io.clone(), &[], first);
    let registration = crate::iceberg::ArchiveRegistration::new(
        [IcebergDataFile::new("s3://archive/out/b.parquet", 1, 1)],
        Some(vec!["s3://archive/raw/b.parquet".to_string()]),
    )
    .unwrap();
    let second = ClaimIndex::append(&table, &registration, None)
        .await
        .unwrap();
    let table = MetadataTable::with_properties_on(file_io, &[], second);

    let loaded = ClaimIndex::load(&table).await.unwrap();
    assert_eq!(
        loaded.sources,
        HashSet::from([
            "s3://archive/raw/a.parquet".to_string(),
            "s3://archive/raw/b.parquet".to_string()
        ])
    );
    let found = ClaimIndex::find_cached(
        &table,
        None,
        &["s3://archive/raw/a.parquet".to_string()],
        &[],
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        found.sources,
        HashSet::from(["s3://archive/raw/a.parquet".to_string()])
    );
}

#[tokio::test]
async fn claim_indexes_find_and_retain_archive_batches() {
    let table = MetadataTable::with_batches(&[]);
    let file_io = table.file_io().clone();
    let live = ClaimBatch::from_claims(
        HashSet::from(["s3://archive/raw/live.parquet".to_string()]),
        HashSet::from(["s3://archive/out/live.parquet".to_string()]),
    );
    let dead = ClaimBatch::from_claims(
        HashSet::from(["s3://archive/raw/dead.parquet".to_string()]),
        HashSet::from(["s3://archive/out/dead.parquet".to_string()]),
    );
    let properties =
        ClaimIndexPage::write_consolidated_batches(&table, vec![live.clone(), dead.clone()], 1)
            .await
            .unwrap();
    let table = MetadataTable::with_properties_on(file_io.clone(), &[], properties);

    let found = ClaimIndex::find_batch(&table, None, &live.digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found, live);

    let loaded = ClaimIndex::load(&table).await.unwrap();
    assert_eq!(loaded.batches.len(), 2);
    let retained = loaded.retained(&HashSet::from(
        ["s3://archive/out/live.parquet".to_string()],
    ));
    assert_eq!(
        retained.batches.keys().cloned().collect::<HashSet<_>>(),
        HashSet::from([live.digest.clone()])
    );
    assert!(!retained.outputs.contains("s3://archive/out/dead.parquet"));

    let registration = crate::iceberg::ArchiveRegistration::new(
        [IcebergDataFile::new("s3://archive/out/new.parquet", 1, 1)],
        Some(vec!["s3://archive/raw/new.parquet".to_string()]),
    )
    .unwrap();
    let appended = ClaimIndex::append(&table, &registration, None)
        .await
        .unwrap();
    let table = MetadataTable::with_properties_on(file_io, &[], appended);
    let new_batch = ClaimBatch::from_claims(
        HashSet::from(["s3://archive/raw/new.parquet".to_string()]),
        HashSet::from(["s3://archive/out/new.parquet".to_string()]),
    );
    assert_eq!(
        ClaimIndex::find_batch(&table, None, &new_batch.digest)
            .await
            .unwrap()
            .unwrap(),
        new_batch
    );
}

#[tokio::test]
async fn metadata_reachability_covers_the_current_file_and_every_index_page() {
    let table = MetadataTable::with_batches(&[]);
    let file_io = table.file_io().clone();
    let properties = ClaimIndexPage::write_consolidated(
        &table,
        HashSet::from(["s3://archive/raw/a.parquet".to_string()]),
        HashSet::from(["s3://archive/out/a.parquet".to_string()]),
        1,
    )
    .await
    .unwrap();
    let head = properties.get(INDEX_HEAD_KEY).unwrap().clone();
    let table = MetadataTable::with_properties_on(file_io, &[], properties);

    let reachable = IcebergCatalog::reachable_metadata(&table).await.unwrap();
    assert!(reachable.contains("s3://archive/table/metadata/v1.json"));
    assert!(reachable.contains(&head));
    assert!(reachable.len() >= 2);
}

#[test]
fn metadata_cleanup_lists_the_parent_directory_of_the_current_metadata_file() {
    let (bucket, prefix) =
        IcebergCatalog::metadata_listing_prefix("s3://archive/table/metadata/v1.json").unwrap();

    assert_eq!(bucket, Bucket::new("archive"));
    assert_eq!(prefix, ObjectKey::new("table/metadata/"));
}

#[test]
fn metadata_cleanup_only_selects_unreachable_objects_beyond_the_safety_window() {
    let bucket = Bucket::new("archive");
    let live = S3Object {
        key: ObjectKey::new("table/metadata/live.json"),
        last_modified_millis: Some(10),
        size: 1,
    };
    let old = S3Object {
        key: ObjectKey::new("table/metadata/archive-index-old.json"),
        last_modified_millis: Some(10),
        size: 1,
    };
    let fresh = S3Object {
        key: ObjectKey::new("table/metadata/archive-index-fresh.json"),
        last_modified_millis: Some(100),
        size: 1,
    };
    let reachable = HashSet::from(["s3://archive/table/metadata/live.json".to_string()]);
    let candidates = IcebergCatalog::metadata_maintenance_candidates(
        &bucket,
        &[live, old.clone(), fresh],
        &reachable,
        TimestampMilliseconds(50),
    );

    assert_eq!(candidates, vec![old]);
}

#[tokio::test]
async fn metadata_sweep_paginates_and_deletes_only_unreachable_old_objects() {
    let (address, _submitted, _server) = catalog_config_server();
    let catalog = IcebergCatalog::connect(&crate::iceberg::IcebergConfig {
        catalog_uri: address,
        warehouse: "s3://archive/warehouse".to_string(),
        token: None,
        namespace: "ns".to_string(),
    })
    .await
    .unwrap();
    let base = MetadataTable::with_batches(&[]);
    let file_io = base.file_io().clone();
    let properties = ClaimIndexPage::write_consolidated(
        &base,
        HashSet::from(["s3://archive/raw/a.parquet".to_string()]),
        HashSet::from(["s3://archive/out/a.parquet".to_string()]),
        1,
    )
    .await
    .unwrap();
    let head = properties.get(INDEX_HEAD_KEY).unwrap().clone();
    let table = MetadataTable::with_properties_on(file_io, &[], properties);
    let head_key = head.strip_prefix("s3://archive/").unwrap().to_string();
    let old_key = "table/metadata/orphan-old.json";
    let fresh_key = "table/metadata/orphan-fresh.json";
    let now = TimestampMilliseconds::now().0;
    let old = now - 3 * 24 * 60 * 60 * 1_000;
    let pages = vec![
        vec![
            FakeObjectStore::object(head_key, old),
            FakeObjectStore::object(old_key, old),
        ],
        vec![
            FakeObjectStore::object("table/metadata/v1.json", old),
            FakeObjectStore::object(fresh_key, now),
        ],
    ];
    let store = FakeObjectStore::with_pages(pages);

    let deleted = catalog
        .sweep_metadata(&table, &store, std::time::Duration::from_secs(24 * 60 * 60))
        .await
        .unwrap();

    assert_eq!(deleted, 1);
    assert_eq!(store.deleted(), vec![old_key.to_string()]);
    assert_eq!(
        store.observed_continuations(),
        vec![None, Some("page-1".to_string())]
    );
}

#[tokio::test]
async fn large_consolidated_claim_indexes_are_sharded_into_multiple_blocks() {
    let table = MetadataTable::with_batches(&[]);
    let file_io = table.file_io().clone();
    let sources = (0..6_000)
        .map(|index| format!("s3://archive/raw/{index:0>6}-{}.parquet", "x".repeat(180)))
        .collect::<HashSet<_>>();
    let properties = ClaimIndexPage::write_consolidated(
        &table,
        sources.clone(),
        HashSet::from(["s3://archive/out/a.parquet".to_string()]),
        1,
    )
    .await
    .unwrap();
    let root = IndexParent {
        location: properties.get(INDEX_HEAD_KEY).unwrap().clone(),
        digest: properties.get(INDEX_HEAD_DIGEST_KEY).unwrap().clone(),
    };
    let page = ClaimIndexPage::read(&table, &root, None).await.unwrap();
    assert!(page.blocks.len() > 1);

    let table = MetadataTable::with_properties_on(file_io, &[], properties);
    let loaded = ClaimIndex::load(&table).await.unwrap();
    assert_eq!(loaded.sources, sources);
}

#[tokio::test]
async fn compacted_metadata_without_a_claim_block_is_a_hard_fault() {
    let files = [IcebergDataFile::new("s3://archive/out/a.parquet", 1, 2)];
    let table = MetadataTable::with_batches(&[&files]);

    assert!(IcebergCatalog::source_files_from_metadata(&table, true).is_err());
    assert!(
        IcebergCatalog::source_files_from_metadata(&table, false)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn claim_index_validation_rejects_unsupported_versions_and_partial_heads() {
    let unsupported = MetadataTable::with_properties(
        &[],
        HashMap::from([
            (INDEX_HEAD_KEY.to_string(), "s3://archive/head".to_string()),
            (INDEX_HEAD_DIGEST_KEY.to_string(), "0".repeat(64)),
            (
                super::claim_index::INDEX_FORMAT_VERSION_KEY.to_string(),
                "1".to_string(),
            ),
        ]),
    );
    assert!(IcebergCatalog::validate_claim_index(&unsupported).is_err());

    let partial = MetadataTable::with_properties(
        &[],
        HashMap::from([(INDEX_HEAD_KEY.to_string(), "s3://archive/head".to_string())]),
    );
    assert!(IcebergCatalog::validate_claim_index(&partial).is_err());
}

struct DeniedAuthorizer;

#[async_trait::async_trait]
impl platform_lease::CommitAuthorizer for DeniedAuthorizer {
    async fn authorize(&self) -> platform_errors::PlatformResult<()> {
        Err(platform_errors::PlatformError::external(
            "lease was lost before property commit",
        ))
    }
}

fn catalog_config_server() -> (
    String,
    std::sync::mpsc::Receiver<()>,
    std::thread::JoinHandle<()>,
) {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (sender, receiver) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0_u8; 4096];
        let _ = stream.read(&mut buffer);
        let body = r#"{"defaults":{},"overrides":{}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
        listener.set_nonblocking(true).unwrap();
        for _ in 0..20 {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.read(&mut buffer);
                    sender.send(()).unwrap();
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        }
    });
    (address, receiver, handle)
}

#[tokio::test]
async fn a_lost_lease_blocks_property_updates_before_they_are_submitted() {
    let (address, submitted, server) = catalog_config_server();
    let catalog = IcebergCatalog::connect(&crate::iceberg::IcebergConfig {
        catalog_uri: address,
        warehouse: "s3://archive/warehouse".to_string(),
        token: None,
        namespace: "ns".to_string(),
    })
    .await
    .unwrap();
    let table = MetadataTable::with_batches(&[]);
    let context = CommitContext::default()
        .with_lease("compact:events:prefix", "owner-a", 7)
        .with_authorizer(Arc::new(DeniedAuthorizer));

    assert!(
        catalog
            .ensure_required_properties(table, &context)
            .await
            .is_err()
    );
    assert!(
        submitted
            .recv_timeout(std::time::Duration::from_millis(300))
            .is_err()
    );
    server.join().unwrap();
}

#[tokio::test]
async fn claim_index_consolidation_starts_at_the_depth_threshold() {
    let mut properties = HashMap::from([
        (
            INDEX_HEAD_KEY.to_string(),
            "s3://archive/table/metadata/head.json".to_string(),
        ),
        (INDEX_HEAD_DIGEST_KEY.to_string(), "digest".to_string()),
        (
            super::claim_index::INDEX_FORMAT_VERSION_KEY.to_string(),
            ClaimIndexPage::format_version().to_string(),
        ),
        (
            super::claim_index::INDEX_DEPTH_KEY.to_string(),
            "63".to_string(),
        ),
    ]);
    let shallow = MetadataTable::with_properties(&[], properties.clone());
    assert!(!ClaimIndex::needs_consolidation(&shallow));

    properties.insert(
        super::claim_index::INDEX_DEPTH_KEY.to_string(),
        "64".to_string(),
    );
    let deep = MetadataTable::with_properties(&[], properties);
    assert!(ClaimIndex::needs_consolidation(&deep));
}

#[tokio::test]
async fn the_first_indexed_commit_installs_finite_retention_properties() {
    let table = MetadataTable::with_batches(&[]);
    let table_properties = HashMap::from([(
        INDEX_HEAD_KEY.to_string(),
        "s3://archive/table/metadata/head.json".to_string(),
    )]);

    let commit =
        SnapshotCommit::build_with_table_properties(&table, &[], HashMap::new(), table_properties)
            .await
            .unwrap();
    let request = commit.to_request(&TableIdent::from_strs(["ns", "table"]).unwrap());
    let properties = request["updates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|update| update["action"] == "set-properties")
        .unwrap();

    assert_eq!(
        properties["updates"]["history.expire.min-snapshots-to-keep"],
        "10"
    );
    assert_eq!(
        properties["updates"]["history.expire.max-snapshot-age-ms"],
        (30 * 24 * 60 * 60 * 1_000_i64).to_string()
    );
}

#[tokio::test]
async fn an_unreferenced_index_page_waits_for_the_safety_window() {
    let table = MetadataTable::with_batches(&[]);
    let properties = ClaimIndexPage::write_consolidated(
        &table,
        HashSet::from(["s3://archive/raw/a.parquet".to_string()]),
        HashSet::from(["s3://archive/out/a.parquet".to_string()]),
        1,
    )
    .await
    .unwrap();
    let head = properties.get(INDEX_HEAD_KEY).unwrap().clone();
    let (bucket, key) = S3::parse_uri(&head).unwrap();
    let orphan = S3Object {
        key,
        last_modified_millis: Some(100),
        size: 1,
    };
    let reachable = IcebergCatalog::reachable_metadata(&table).await.unwrap();

    assert!(!reachable.contains(&head));
    assert!(
        IcebergCatalog::metadata_maintenance_candidates(
            &bucket,
            std::slice::from_ref(&orphan),
            &reachable,
            TimestampMilliseconds(50),
        )
        .is_empty()
    );
    assert_eq!(
        IcebergCatalog::metadata_maintenance_candidates(
            &bucket,
            std::slice::from_ref(&orphan),
            &reachable,
            TimestampMilliseconds(150),
        ),
        vec![orphan]
    );
}

#[tokio::test]
async fn compacted_metadata_round_trips_source_claims() {
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];
    let sources = vec!["s3://archive/raw/source.parquet".to_string()];
    let properties = IcebergCatalog::snapshot_properties(&files, Some(&sources)).unwrap();
    let table = MetadataTable::with_summaries(&[(&files, properties)]);

    assert_eq!(
        IcebergCatalog::source_files_from_metadata(&table, true).unwrap(),
        HashSet::from_iter(sources)
    );
}

#[tokio::test]
async fn damaged_source_claims_fail_metadata_loading() {
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];
    let sources = vec!["s3://archive/raw/source.parquet".to_string()];
    let properties = IcebergCatalog::snapshot_properties(&files, Some(&sources)).unwrap();

    let mut missing_source = properties.clone();
    missing_source.remove("source-file:s3://archive/raw/source.parquet");
    let missing_source = MetadataTable::with_summaries(&[(&files, missing_source)]);
    assert!(IcebergCatalog::source_files_from_metadata(&missing_source, true).is_err());

    let mut wrong_count = properties.clone();
    wrong_count.insert("source-claim-count".to_string(), "2".to_string());
    let wrong_count = MetadataTable::with_summaries(&[(&files, wrong_count)]);
    assert!(IcebergCatalog::source_files_from_metadata(&wrong_count, true).is_err());

    let mut wrong_version = properties;
    wrong_version.insert("archive-claim-version".to_string(), "2".to_string());
    let wrong_version = MetadataTable::with_summaries(&[(&files, wrong_version)]);
    assert!(IcebergCatalog::source_files_from_metadata(&wrong_version, true).is_err());
}
