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
