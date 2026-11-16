use crate::{ArchiveKind, SourceBatch};
use platform_types::TimestampMilliseconds;

fn batch(files: &[&str]) -> SourceBatch {
    SourceBatch {
        kind: ArchiveKind::Events,
        prefix: "raw/kind=events".to_string(),
        bucket_start: TimestampMilliseconds(0),
        files: files.iter().map(|file| file.to_string()).collect(),
    }
}

#[test]
fn digest_is_independent_of_file_order() {
    assert_eq!(
        batch(&["b.parquet", "a.parquet"]).digest(),
        batch(&["a.parquet", "b.parquet"]).digest()
    );
}

#[test]
fn digest_changes_with_the_source_set() {
    assert_ne!(
        batch(&["a.parquet"]).digest(),
        batch(&["a.parquet", "b.parquet"]).digest()
    );
}

#[test]
fn digest_is_full_sha256_for_singleton_and_duplicate_sets() {
    let singleton = batch(&["a.parquet"]).digest();
    let duplicate = batch(&["a.parquet", "a.parquet"]).digest();

    assert_eq!(singleton.len(), 64);
    assert_eq!(singleton, duplicate);
}

#[test]
fn digest_uses_generated_source_digests() {
    assert_eq!(
        SourceBatch::digest_files(&[
            "s3://bucket/a.parquet".to_string(),
            "s3://bucket/b.parquet".to_string(),
        ]),
        SourceBatch::digest_files(&[
            "s3://bucket/b.parquet".to_string(),
            "s3://bucket/a.parquet".to_string(),
            "s3://bucket/b.parquet".to_string(),
        ])
    );
}

#[test]
fn prefix_digest_is_plain_sha256_of_the_prefix() {
    let digest = SourceBatch::prefix_digest("raw/events/hour=1");

    assert_eq!(digest.len(), 64);
    assert_eq!(
        digest,
        "65ae433e487e91464cdb2398a373dc6c1876779fd4db5819f94d3330c4d3591f"
    );
}
