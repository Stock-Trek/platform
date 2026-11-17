use crate::{
    ArchiveKind, CompactionId, SOURCE_MANIFEST_FORMAT_VERSION, SourceManifest,
    SourceManifestParquet,
};
use platform_types::TimestampMilliseconds;

fn id() -> CompactionId {
    CompactionId::for_batch(
        ArchiveKind::Events,
        TimestampMilliseconds(1_700_000_000_000),
        &"a".repeat(64),
    )
    .unwrap()
}

const SPEC_ID: i32 = 0;

fn manifest() -> SourceManifest {
    SourceManifest::new(
        id(),
        SPEC_ID,
        vec![
            "s3://bucket/raw/b.parquet".to_string(),
            "s3://bucket/raw/a.parquet".to_string(),
        ],
    )
    .unwrap()
}

#[test]
fn manifests_sort_and_deduplicate_sources() {
    let manifest = SourceManifest::new(
        id(),
        SPEC_ID,
        vec![
            "s3://bucket/b".to_string(),
            "s3://bucket/a".to_string(),
            "s3://bucket/a".to_string(),
        ],
    )
    .unwrap();

    assert_eq!(manifest.sources, ["s3://bucket/a", "s3://bucket/b"]);
    assert_eq!(manifest.source_count(), 2);
    assert!(manifest.contains("s3://bucket/a"));
    assert!(!manifest.contains("s3://bucket/c"));
    assert_eq!(manifest.format_version, SOURCE_MANIFEST_FORMAT_VERSION);
    assert_eq!(manifest.partition_spec_id, SPEC_ID);
}

#[test]
fn manifests_reject_invalid_source_sets() {
    assert!(SourceManifest::new(id(), SPEC_ID, Vec::new()).is_err());
    assert!(SourceManifest::new(id(), SPEC_ID, vec![String::new()]).is_err());
    assert!(SourceManifest::new(id(), -1, vec!["s3://bucket/a".to_string()]).is_err());

    let mut manifest = manifest();
    manifest.sources.reverse();
    assert!(manifest.validate().is_err());

    manifest.sources = vec!["s3://bucket/a".to_string(); 2];
    assert!(manifest.validate().is_err());
}

#[test]
fn manifests_round_trip_through_parquet() {
    let manifest = manifest();
    let bytes = SourceManifestParquet::encode(&manifest).unwrap();

    assert_eq!(SourceManifestParquet::row_count(&bytes).unwrap(), 2);
    assert_eq!(SourceManifestParquet::decode(bytes).unwrap(), manifest);
}
