use super::snapshot::SnapshotCommit;
use crate::iceberg::catalog::{IcebergCatalog, IcebergDataFile};
use iceberg::{
    Runtime, TableIdent,
    io::FileIO,
    spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SortOrder, Summary,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    },
    table::Table,
};
use std::{collections::HashMap, sync::Arc};

struct MetadataTable;

impl MetadataTable {
    fn with_batches(batches: &[&[IcebergDataFile]]) -> Table {
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
            HashMap::new(),
        )
        .unwrap();

        for (index, files) in batches.iter().enumerate() {
            let index = i64::try_from(index).unwrap();
            let snapshot = Snapshot::builder()
                .with_snapshot_id(index + 1)
                .with_sequence_number(index)
                .with_timestamp_ms(index)
                .with_manifest_list(format!("s3://archive/table/metadata/snap-{index}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: IcebergCatalog::snapshot_properties(files, None)
                        .unwrap(),
                })
                .build();
            builder = builder.add_snapshot(snapshot).unwrap();
        }

        Table::builder()
            .file_io(FileIO::new_with_memory())
            .metadata_location("s3://archive/table/metadata/v1.json")
            .metadata(builder.build().unwrap().metadata)
            .identifier(TableIdent::from_strs(["ns", "table"]).unwrap())
            .runtime(Runtime::current())
            .build()
            .unwrap()
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
async fn registered_batch_fingerprints_are_read_from_snapshot_metadata() {
    let first = IcebergDataFile::new("s3://archive/a.parquet", 1, 2);
    let second = IcebergDataFile::new("s3://archive/b.parquet", 3, 4);
    let table = MetadataTable::with_batches(&[&[first.clone(), second.clone()]]);

    assert!(IcebergCatalog::batch_registered_in_metadata(
        &table,
        &[first.clone(), second.clone()]
    ));
    assert!(!IcebergCatalog::batch_registered_in_metadata(
        &table,
        &[second, first]
    ));
    assert!(!IcebergCatalog::batch_registered_in_metadata(&table, &[]));
}

#[test]
fn snapshot_properties_record_source_files_alongside_the_batch_fingerprint() {
    let files = [IcebergDataFile::new("s3://archive/a.parquet", 1, 2)];
    let source_files = ["s3://archive/raw/source.parquet".to_string()];
    let properties = IcebergCatalog::snapshot_properties(&files, Some(&source_files)).unwrap();

    assert_eq!(
        properties.get("archive-batch"),
        Some(&IcebergCatalog::batch_fingerprint(&files))
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
    let commit = SnapshotCommit::build(&table, &files, HashMap::new())
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
    let commit = SnapshotCommit::build(&table, &files, HashMap::new())
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
