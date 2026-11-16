use crate::iceberg::catalog::IcebergDataFile;
use crate::{ContentHasher, SourceBatch};
use platform_errors::{PlatformError, PlatformResult};
use std::collections::HashSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitContext {
    scope: Option<String>,
    owner: Option<String>,
    fencing_token: Option<u64>,
}

impl CommitContext {
    pub fn with_lease(
        mut self,
        scope: impl Into<String>,
        owner: impl Into<String>,
        fencing_token: u64,
    ) -> Self {
        self.scope = Some(scope.into());
        self.owner = Some(owner.into());
        self.fencing_token = Some(fencing_token);
        self
    }

    pub fn scope(&self) -> &str {
        self.scope.as_deref().unwrap_or("")
    }

    pub fn owner(&self) -> &str {
        self.owner.as_deref().unwrap_or("")
    }

    pub fn fencing_token(&self) -> u64 {
        self.fencing_token.unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRegistration {
    files: Vec<IcebergDataFile>,
    source_files: Option<Vec<String>>,
}

impl ArchiveRegistration {
    pub fn new(
        files: impl IntoIterator<Item = IcebergDataFile>,
        source_files: Option<Vec<String>>,
    ) -> PlatformResult<Self> {
        let source_files = source_files
            .map(|mut sources| {
                if sources.is_empty() {
                    return Err(PlatformError::config_invalid(
                        "archive source batch",
                        "must contain at least one source object",
                    ));
                }
                sources.sort();
                sources.dedup();
                Ok(sources)
            })
            .transpose()?;

        let mut files = files.into_iter().collect::<Vec<_>>();
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        files.dedup_by(|left, right| left.file_path == right.file_path);

        if files.is_empty() && source_files.is_none() {
            return Err(PlatformError::config_invalid(
                "archive registration",
                "must contain data files or source objects",
            ));
        }

        Ok(Self {
            files,
            source_files,
        })
    }

    pub fn files(&self) -> &[IcebergDataFile] {
        &self.files
    }

    pub fn source_files(&self) -> Option<&[String]> {
        self.source_files.as_deref()
    }

    pub fn expected_outputs(&self) -> HashSet<String> {
        self.files
            .iter()
            .map(|file| file.file_path.clone())
            .collect()
    }

    pub fn batch_digest(&self) -> String {
        if let Some(sources) = &self.source_files {
            return SourceBatch::digest_files(sources);
        }
        let mut hasher = ContentHasher::new();
        for file in &self.files {
            hasher.update(file.file_path.as_bytes());
            hasher.update([0_u8]);
        }
        hasher.finish().to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictDetails {
    pub claimed_sources: Vec<String>,
    pub live_outputs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterOutcome {
    Applied,
    AlreadyApplied,
    Conflict(ConflictDetails),
}

impl RegisterOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Applied | Self::AlreadyApplied)
    }
}
