use crate::{ArchiveKind, RecoveryInput, RecoveryLayer};
use std::collections::HashSet;

fn sources(values: &[&str]) -> HashSet<String> {
    values.iter().map(|value| value.to_string()).collect()
}

fn base() -> RecoveryInput {
    RecoveryInput {
        kind: ArchiveKind::Events,
        raw_expired: false,
        raw_available: true,
        compacted_available: true,
        ledger_available: true,
        source_manifests_readable: true,
        snapshot_sources: sources(&["s3://bucket/raw/a"]),
        ledger_sources: sources(&["s3://bucket/raw/b"]),
        ledger_entries: Vec::new(),
    }
}

#[test]
fn dual_layer_recovery_unions_snapshot_and_ledger_sources() {
    let decision = base().decide().unwrap();

    assert_eq!(decision.layer, RecoveryLayer::DualLayer);
    assert_eq!(
        decision.processed,
        sources(&["s3://bucket/raw/a", "s3://bucket/raw/b"])
    );
    assert!(!decision.raw_fallback_allowed);
    assert!(decision.ledger_watermark.is_none());
}

#[test]
fn raw_expiry_switches_to_compacted_and_ledger() {
    let mut input = base();
    input.raw_expired = true;
    let decision = input.decide().unwrap();

    assert_eq!(decision.layer, RecoveryLayer::CompactedPlusLedger);
    assert_eq!(
        decision.processed,
        sources(&["s3://bucket/raw/a", "s3://bucket/raw/b"])
    );
}

#[test]
fn recovery_requires_durable_copies_after_raw_expiry() {
    let mut input = base();
    input.raw_expired = true;
    input.compacted_available = false;
    assert!(input.decide().is_err());

    let mut input = base();
    input.raw_expired = true;
    input.ledger_available = false;
    assert!(input.decide().is_err());

    let mut input = base();
    input.raw_available = false;
    input.snapshot_sources.clear();
    input.ledger_sources.clear();
    assert!(input.decide().is_err());
}

#[test]
fn unreadable_source_manifests_quarantine_recovery() {
    let mut input = base();
    input.source_manifests_readable = false;
    assert!(input.decide().is_err());
}
