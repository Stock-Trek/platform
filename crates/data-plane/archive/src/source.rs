use crate::record::ArchiveKind;
use platform_types::new_types::TimestampMilliseconds;

/// A group of raw archive files that can be compacted together.
#[derive(Debug, Clone)]
pub struct SourceBatch {
    pub kind: ArchiveKind,
    pub prefix: String,
    pub bucket_start: TimestampMilliseconds,
    pub files: Vec<String>,
}
