use crate::{ArchiveKind, LedgerEntry};
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryLayer {
    DualLayer,
    CompactedPlusLedger,
}

impl RecoveryLayer {
    pub fn label(self) -> &'static str {
        match self {
            Self::DualLayer => "dual_layer",
            Self::CompactedPlusLedger => "compacted_plus_ledger",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryInput {
    pub kind: ArchiveKind,
    pub raw_expired: bool,
    pub raw_available: bool,
    pub compacted_available: bool,
    pub ledger_available: bool,
    pub source_manifests_readable: bool,
    pub snapshot_sources: HashSet<String>,
    pub ledger_sources: HashSet<String>,
    pub ledger_entries: Vec<LedgerEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryDecision {
    pub kind: ArchiveKind,
    pub layer: RecoveryLayer,
    pub processed: HashSet<String>,
    pub raw_fallback_allowed: bool,
    pub ledger_watermark: Option<TimestampMilliseconds>,
}

impl RecoveryInput {
    pub fn decide(self) -> PlatformResult<RecoveryDecision> {
        if !self.source_manifests_readable {
            return Err(Self::error(
                self.kind,
                "source manifest is missing or corrupt; batch is quarantined",
            ));
        }
        if self.raw_expired && !self.compacted_available {
            return Err(Self::error(
                self.kind,
                "compacted files are unavailable after raw expiry",
            ));
        }
        if self.raw_expired && !self.ledger_available {
            return Err(Self::error(
                self.kind,
                "ledger is unavailable after raw expiry",
            ));
        }
        if !self.raw_expired && !self.raw_available {
            return Err(Self::error(
                self.kind,
                "raw data is unavailable before raw expiry",
            ));
        }

        let layer = if self.raw_expired {
            RecoveryLayer::CompactedPlusLedger
        } else {
            RecoveryLayer::DualLayer
        };
        let mut processed = self.snapshot_sources;
        processed.extend(self.ledger_sources);
        let ledger_watermark = self
            .ledger_entries
            .iter()
            .map(|entry| entry.processed_at)
            .max();
        Ok(RecoveryDecision {
            kind: self.kind,
            layer,
            processed,
            raw_fallback_allowed: false,
            ledger_watermark,
        })
    }

    pub fn from_ledger(
        kind: ArchiveKind,
        raw_expired: bool,
        raw_available: bool,
        compacted_available: bool,
        snapshot_sources: HashSet<String>,
        entries: Vec<LedgerEntry>,
        ledger_sources: HashSet<String>,
    ) -> Self {
        Self {
            kind,
            raw_expired,
            raw_available,
            compacted_available,
            ledger_available: !entries.is_empty(),
            source_manifests_readable: true,
            snapshot_sources,
            ledger_sources,
            ledger_entries: entries,
        }
    }

    fn error(kind: ArchiveKind, message: &str) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("archive recovery for {}: {message}", kind.label()),
        ))
    }
}
