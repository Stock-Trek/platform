use crate::CompactionId;
use platform_errors::{PlatformError, PlatformResult};
use serde::{Deserialize, Serialize};

pub const SOURCE_MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceManifest {
    pub format_version: u32,
    pub compaction_id: CompactionId,
    pub partition_spec_id: i32,
    pub sources: Vec<String>,
}

impl SourceManifest {
    pub fn new(
        compaction_id: CompactionId,
        partition_spec_id: i32,
        sources: Vec<String>,
    ) -> PlatformResult<Self> {
        let mut sources = sources;
        sources.sort();
        sources.dedup();
        let manifest = Self {
            format_version: SOURCE_MANIFEST_FORMAT_VERSION,
            compaction_id,
            partition_spec_id,
            sources,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> PlatformResult<()> {
        if self.format_version != SOURCE_MANIFEST_FORMAT_VERSION {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unsupported source manifest format version {}",
                    self.format_version
                ),
            )));
        }
        if self.partition_spec_id < 0 {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source manifest has no partition spec id",
            )));
        }
        if self.sources.is_empty() {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source manifest has no sources",
            )));
        }
        if self.sources.iter().any(|source| source.is_empty()) {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source manifest contains an empty source URI",
            )));
        }
        if self.sources.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source manifest sources are not sorted and unique",
            )));
        }
        Ok(())
    }

    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    pub fn contains(&self, source: &str) -> bool {
        self.sources
            .binary_search_by(|candidate| candidate.as_str().cmp(source))
            .is_ok()
    }
}
