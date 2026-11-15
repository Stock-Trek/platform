use crate::ArchiveKind;
use platform_types::TimestampMilliseconds;

/// A group of raw archive files that can be compacted together.
#[derive(Debug, Clone)]
pub struct SourceBatch {
    pub kind: ArchiveKind,
    pub prefix: String,
    pub bucket_start: TimestampMilliseconds,
    pub files: Vec<String>,
}

impl SourceBatch {
    pub fn digest(&self) -> String {
        let mut files = self.files.clone();
        files.sort();
        files.dedup();
        let mut hasher = crate::ContentHasher::new();
        for file in files {
            hasher.update(file.as_bytes());
            hasher.update([0_u8]);
        }
        hasher.finish().to_string()
    }
}
