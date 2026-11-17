use crate::{
    ArchiveKind, CompactionId, HOUR_MILLIS, LEDGER_FORMAT_VERSION, LedgerEntry, LedgerParquet,
    SourceManifest,
};
use platform_types::TimestampMilliseconds;

fn manifest(kind: ArchiveKind) -> SourceManifest {
    let id = CompactionId::for_batch(
        kind,
        TimestampMilliseconds(1_700_000_123_456),
        &"b".repeat(64),
    )
    .unwrap();
    SourceManifest::new(id, vec!["s3://bucket/raw/a.parquet".to_string()]).unwrap()
}

fn entry(kind: ArchiveKind) -> LedgerEntry {
    let manifest = manifest(kind);
    LedgerEntry::from_manifest(
        kind,
        TimestampMilliseconds(1_700_000_123_456),
        "s3://bucket/metadata/source-manifests/a/1.parquet".to_string(),
        &manifest,
        TimestampMilliseconds(1_700_000_999_000),
    )
    .unwrap()
}

#[test]
fn entries_align_to_the_processing_hour() {
    let entry = entry(ArchiveKind::Events);

    assert_eq!(
        entry.partition_hour_millis,
        1_700_000_123_456 / HOUR_MILLIS * HOUR_MILLIS
    );
    assert_eq!(entry.partition_hour_millis % HOUR_MILLIS, 0);
    assert_eq!(entry.source_count, 1);
    assert_eq!(entry.format_version, LEDGER_FORMAT_VERSION);
    assert!(!entry.is_older_than(
        TimestampMilliseconds(1_700_000_999_000),
        std::time::Duration::from_secs(1)
    ));
    assert!(entry.is_older_than(
        TimestampMilliseconds(1_800_000_000_000),
        std::time::Duration::from_secs(1)
    ));
}

#[test]
fn entries_reject_misaligned_hours_and_empty_manifests() {
    let mut entry = entry(ArchiveKind::Candles);
    entry.partition_hour_millis += 1;
    assert!(entry.validate().is_err());

    entry.partition_hour_millis = 0;
    entry.source_manifest_uri.clear();
    assert!(entry.validate().is_err());

    entry.source_manifest_uri = "s3://bucket/manifest".to_string();
    entry.source_count = 0;
    assert!(entry.validate().is_err());
}

#[test]
fn entries_round_trip_through_parquet() {
    let entries = vec![entry(ArchiveKind::Events), entry(ArchiveKind::Candles)];
    let bytes = LedgerParquet::encode(&entries).unwrap();

    assert_eq!(LedgerParquet::decode(bytes).unwrap(), entries);
}

#[test]
fn empty_ledger_batches_are_rejected() {
    assert!(LedgerParquet::encode(&[]).is_err());
}
