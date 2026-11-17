use crate::ArchiveKind;
use crate::iceberg::catalog::iceberg_catalog::RawCleanupPlan;
use platform_types::TimestampMilliseconds;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

#[test]
fn removes_old_processed_entries_and_expires_only_unreferenced_objects() {
    let now = TimestampMilliseconds(1_000);
    let processed = HashMap::from([
        (
            "s3://archive/old-table.parquet".to_string(),
            TimestampMilliseconds(900),
        ),
        (
            "s3://archive/old-object.parquet".to_string(),
            TimestampMilliseconds(900),
        ),
        (
            "s3://archive/young.parquet".to_string(),
            TimestampMilliseconds(990),
        ),
    ]);
    let current = HashSet::from([
        "s3://archive/old-table.parquet".to_string(),
        "s3://archive/old-object.parquet".to_string(),
        "s3://archive/young.parquet".to_string(),
    ]);
    let retained = HashSet::from([
        "s3://archive/old-table.parquet".to_string(),
        "s3://archive/young.parquet".to_string(),
    ]);
    let plan = RawCleanupPlan::from_processed(
        ArchiveKind::Events,
        processed,
        &current,
        &retained,
        now,
        Duration::from_millis(50),
    );
    assert_eq!(
        plan.raw_entries,
        vec!["s3://archive/old-object.parquet".to_string()]
    );
    assert_eq!(
        plan.objects,
        vec!["s3://archive/old-object.parquet".to_string()]
    );
}

#[test]
fn retains_objects_referenced_by_any_retained_snapshot() {
    let now = TimestampMilliseconds(1_000);
    let processed = HashMap::from([(
        "s3://archive/old.parquet".to_string(),
        TimestampMilliseconds(900),
    )]);
    let current = HashSet::from(["s3://archive/old.parquet".to_string()]);
    let plan = RawCleanupPlan::from_processed(
        ArchiveKind::Events,
        processed,
        &current,
        &current,
        now,
        Duration::from_millis(50),
    );
    assert!(plan.is_empty());
}
