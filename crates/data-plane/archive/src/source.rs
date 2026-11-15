use crate::ArchiveKind;
use platform_types::TimestampMilliseconds;
use sha2::{Digest, Sha256};

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
        Self::digest_files(&self.files)
    }

    pub fn digest_files(files: &[String]) -> String {
        let mut files = files.to_vec();
        files.sort();
        files.dedup();
        let mut hasher = Sha256::new();
        for file in files {
            hasher.update(file.as_bytes());
            hasher.update([0_u8]);
        }
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut digest = String::with_capacity(64);
        for byte in hasher.finalize() {
            digest.push(HEX[(byte >> 4) as usize] as char);
            digest.push(HEX[(byte & 0x0f) as usize] as char);
        }
        digest
    }
}
