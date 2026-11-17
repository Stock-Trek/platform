use crate::{ArchiveCatalog, ArchiveHandoff, ArchiveIcebergTable, ArchiveKind, RecoveryInput};
use platform_errors::PlatformResult;
use platform_metrics::MarketDataMetrics;
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;
use tracing::info;

/// The compacted/raw handoff together with the authoritative recovery-layer
/// decision for one archive kind.
///
/// Recovery reads the compacted snapshot, the raw table, and the compaction
/// ledger, then records which layer it selected in a structured log and in
/// metrics, including the ledger watermark and the raw expiry state. After raw
/// expiry the compacted layer plus the ledger is the sole authority and raw
/// fallback is never allowed.
pub struct RecoveryHandoff {
    pub handoff: ArchiveHandoff,
    pub kind: ArchiveKind,
    pub layer_label: &'static str,
    pub raw_expired: bool,
    pub ledger_watermark: Option<TimestampMilliseconds>,
}

impl RecoveryHandoff {
    /// Discovers the handoff and decides the authoritative recovery layer.
    pub async fn discover(catalog: &dyn ArchiveCatalog, kind: ArchiveKind) -> PlatformResult<Self> {
        let (compacted, snapshot_sources) = catalog
            .compacted_snapshot(ArchiveIcebergTable::compacted(kind))
            .await?;
        let raw = catalog.data_files(ArchiveIcebergTable::raw(kind)).await?;
        let ledger_entries = catalog.ledger_entries(kind).await?;
        let (ledger_sources, manifests_readable) = Self::ledger_sources(catalog, kind).await;

        let raw_expired = raw.is_empty() && !ledger_entries.is_empty();
        let input = RecoveryInput {
            kind,
            raw_expired,
            raw_available: true,
            compacted_available: !compacted.is_empty(),
            ledger_available: !ledger_entries.is_empty(),
            source_manifests_readable: manifests_readable,
            snapshot_sources,
            ledger_sources,
            ledger_entries: ledger_entries.clone(),
        };
        let decision = input.decide()?;

        let handoff = ArchiveHandoff {
            compacted: Self::sorted_by_path(compacted),
            raw_tail: Self::sorted_by_path(
                raw.into_iter()
                    .filter(|file| !decision.processed.contains(&file.file_path))
                    .collect(),
            ),
        };
        Self::observe(kind, &decision, &handoff);
        Ok(Self {
            handoff,
            kind,
            layer_label: decision.layer.label(),
            raw_expired: decision.raw_expired,
            ledger_watermark: decision.ledger_watermark,
        })
    }
    /// Reads the ledger source set, reporting whether every source manifest
    /// was readable.
    async fn ledger_sources(
        catalog: &dyn ArchiveCatalog,
        kind: ArchiveKind,
    ) -> (HashSet<String>, bool) {
        match catalog.ledger_source_files(kind).await {
            Ok(sources) => (sources, true),
            Err(error) => {
                info!(
                    kind = kind.label(),
                    %error,
                    "archive recovery cannot read every source manifest; quarantining recovery"
                );
                (HashSet::new(), false)
            }
        }
    }

    fn sorted_by_path(mut files: Vec<crate::IcebergDataFile>) -> Vec<crate::IcebergDataFile> {
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        files.dedup_by(|left, right| left.file_path == right.file_path);
        files
    }

    fn observe(kind: ArchiveKind, decision: &crate::RecoveryDecision, handoff: &ArchiveHandoff) {
        MarketDataMetrics::record_recovery_layer(decision.layer.label(), kind.label());
        if let Some(watermark) = decision.ledger_watermark {
            MarketDataMetrics::record_recovery_ledger_watermark(
                kind.label(),
                TimestampMilliseconds::now().0.saturating_sub(watermark.0),
            );
        }
        MarketDataMetrics::record_raw_expiry_decision(
            if decision.raw_expired {
                "raw_expired"
            } else {
                "raw_retained"
            },
            kind.label(),
        );
        info!(
            kind = kind.label(),
            layer = decision.layer.label(),
            raw_expired = decision.raw_expired,
            ledger_watermark = decision.ledger_watermark.map(|watermark| watermark.0),
            raw_fallback_allowed = decision.raw_fallback_allowed,
            compacted_files = handoff.compacted.len(),
            raw_tail_files = handoff.raw_tail.len(),
            "archive recovery selected its authoritative layer"
        );
    }
}
