use crate::{ArchiveKind, MaintenanceStage};
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MaintenanceFileKind {
    Raw,
    Compacted,
    Ledger,
    SourceManifest,
    Manifest,
    ManifestList,
    Metadata,
}

impl MaintenanceFileKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Compacted => "compacted",
            Self::Ledger => "ledger",
            Self::SourceManifest => "source_manifest",
            Self::Manifest => "manifest",
            Self::ManifestList => "manifest_list",
            Self::Metadata => "metadata",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceCandidate {
    pub uri: String,
    pub kind: ArchiveKind,
    pub file_kind: MaintenanceFileKind,
    pub modified_at: TimestampMilliseconds,
}

impl MaintenanceCandidate {
    pub fn new(
        uri: String,
        kind: ArchiveKind,
        file_kind: MaintenanceFileKind,
        modified_at: TimestampMilliseconds,
    ) -> Self {
        Self {
            uri,
            kind,
            file_kind,
            modified_at,
        }
    }

    pub fn age(&self, now: TimestampMilliseconds) -> Duration {
        Duration::from_millis(
            u64::try_from(now.0.saturating_sub(self.modified_at.0)).unwrap_or_default(),
        )
    }

    pub fn is_older_than(&self, now: TimestampMilliseconds, grace: Duration) -> bool {
        self.age(now) >= grace
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenancePlan {
    pub stage: MaintenanceStage,
    pub candidates: Vec<MaintenanceCandidate>,
}

impl MaintenancePlan {
    pub fn dry_run(stage: MaintenanceStage, candidates: Vec<MaintenanceCandidate>) -> Self {
        Self { stage, candidates }
    }

    pub fn deletion_candidates(
        &self,
        now: TimestampMilliseconds,
        grace: Duration,
        floor: Duration,
    ) -> PlatformResult<Vec<MaintenanceCandidate>> {
        let selected = self
            .candidates
            .iter()
            .filter(|candidate| candidate.is_older_than(now, grace))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(young) = selected
            .iter()
            .find(|candidate| !candidate.is_older_than(now, floor))
        {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "refusing maintenance deletion for {} because it is younger than the retention floor",
                    young.uri
                ),
            )));
        }
        Ok(selected)
    }

    pub fn oldest_age(&self, now: TimestampMilliseconds) -> Option<Duration> {
        self.candidates
            .iter()
            .map(|candidate| candidate.age(now))
            .max()
    }

    pub fn youngest_age(&self, now: TimestampMilliseconds) -> Option<Duration> {
        self.candidates
            .iter()
            .map(|candidate| candidate.age(now))
            .min()
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }
}
