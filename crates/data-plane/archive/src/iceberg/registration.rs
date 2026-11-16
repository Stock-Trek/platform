use crate::iceberg::catalog::IcebergDataFile;
use crate::{ContentHasher, SourceBatch};
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::CommitAuthorizer;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct CommitContext {
    scope: Option<String>,
    owner: Option<String>,
    fencing_token: Option<u64>,
    authorizer: Option<Arc<dyn CommitAuthorizer>>,
}

impl fmt::Debug for CommitContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommitContext")
            .field("scope", &self.scope)
            .field("owner", &self.owner)
            .field("fencing_token", &self.fencing_token)
            .field("authorized", &self.authorizer.is_some())
            .finish()
    }
}

impl PartialEq for CommitContext {
    fn eq(&self, other: &Self) -> bool {
        self.scope == other.scope
            && self.owner == other.owner
            && self.fencing_token == other.fencing_token
    }
}

impl Eq for CommitContext {}

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

    pub fn with_authorizer(mut self, authorizer: Arc<dyn CommitAuthorizer>) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    pub async fn authorize(&self) -> PlatformResult<()> {
        match &self.authorizer {
            Some(authorizer) => authorizer.authorize().await,
            None => Ok(()),
        }
    }

    pub fn remaining(&self) -> Option<std::time::Duration> {
        self.authorizer
            .as_ref()
            .and_then(|authorizer| authorizer.remaining())
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
        let mut hasher = ContentHasher::new();
        if let Some(sources) = &self.source_files {
            hasher.update(SourceBatch::digest_files(sources).as_bytes());
        }
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
