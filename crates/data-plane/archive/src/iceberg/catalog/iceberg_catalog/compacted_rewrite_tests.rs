use crate::iceberg::catalog::iceberg_catalog::CompactedRewritePlan;
use crate::iceberg::{ArchiveIcebergTable, IcebergDataFile, IcebergFilePartition};
use crate::{ArchiveHour, ArchiveKind, PartitionTarget};
use platform_types::TimestampMilliseconds;

fn file(path: &str, size: i64, hour: i64) -> IcebergDataFile {
    IcebergDataFile::partitioned(
        path,
        size,
        1,
        IcebergFilePartition::new(ArchiveKind::Events, hour),
    )
}

#[test]
fn plans_one_bounded_rewrite_per_exceeded_partition() {
    let first = ArchiveIcebergTable::compacted(ArchiveKind::Events);
    let files = vec![
        file("s3://archive/a.parquet", 10, 0),
        file("s3://archive/b.parquet", 10, 0),
        file("s3://archive/c.parquet", 10, 0),
        file("s3://archive/d.parquet", 10, 1),
    ];
    let plan = CompactedRewritePlan::from_files(
        first,
        &files,
        PartitionTarget {
            max_data_files: 2,
            max_bytes: 100,
        },
    );
    assert_eq!(plan.candidates.len(), 1);
    assert_eq!(plan.candidates[0].input_files.len(), 3);
    assert_eq!(plan.candidates[0].partition.hour_millis, 0);
}

#[test]
fn stops_selecting_files_at_the_size_target() {
    let table = ArchiveIcebergTable::compacted(ArchiveKind::Events);
    let files = vec![
        file("s3://archive/a.parquet", 40, 0),
        file("s3://archive/b.parquet", 40, 0),
        file("s3://archive/c.parquet", 40, 0),
    ];
    let plan = CompactedRewritePlan::from_files(
        table,
        &files,
        PartitionTarget {
            max_data_files: 2,
            max_bytes: 100,
        },
    );
    assert_eq!(plan.candidates[0].input_files.len(), 2);
}

#[test]
fn leaves_partitions_within_targets_alone() {
    let table = ArchiveIcebergTable::compacted(ArchiveKind::Events);
    let files = vec![
        file("s3://archive/a.parquet", 10, 0),
        file("s3://archive/b.parquet", 10, 0),
    ];
    let plan = CompactedRewritePlan::from_files(
        table,
        &files,
        PartitionTarget {
            max_data_files: 2,
            max_bytes: 100,
        },
    );
    assert!(plan.is_empty());
}

#[test]
fn derives_compacted_partitions_from_hourly_object_paths() {
    let hour = ArchiveHour::from_timestamp(TimestampMilliseconds(3_600_000))
        .unwrap()
        .path();
    let first = IcebergDataFile::new(
        format!("s3://archive/compacted/events/symbol/bucket=0/{hour}/a.parquet"),
        10,
        1,
    );
    let second = IcebergDataFile::new(
        format!("s3://archive/compacted/events/symbol/bucket=0/{hour}/b.parquet"),
        10,
        1,
    );
    let plan = CompactedRewritePlan::from_files(
        ArchiveIcebergTable::compacted(ArchiveKind::Events),
        &[first, second],
        PartitionTarget {
            max_data_files: 1,
            max_bytes: 100,
        },
    );
    assert_eq!(plan.candidates.len(), 1);
    assert_eq!(plan.candidates[0].partition.hour_millis, 3_600_000);
}

#[test]
fn derives_ledger_partitions_from_ledger_object_paths() {
    let first = IcebergDataFile::new(
        "s3://archive/ledger/events/data/events/hour=3600000/processed-a.parquet",
        10,
        1,
    );
    let second = IcebergDataFile::new(
        "s3://archive/ledger/events/data/events/hour=3600000/processed-b.parquet",
        10,
        1,
    );
    let plan = CompactedRewritePlan::from_files(
        ArchiveIcebergTable::ledger(ArchiveKind::Events),
        &[first, second],
        PartitionTarget {
            max_data_files: 1,
            max_bytes: 100,
        },
    );
    assert_eq!(plan.candidates.len(), 1);
    assert_eq!(plan.candidates[0].partition.hour_millis, 3_600_000);
}
