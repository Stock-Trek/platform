use crate::{RawDeletionDecision, RawExpiryInput};
use platform_types::TimestampMilliseconds;
use std::time::Duration;

fn base() -> RawExpiryInput {
    RawExpiryInput {
        now: TimestampMilliseconds(2_000_000_000_000),
        processed_at: TimestampMilliseconds(1_000_000_000_000),
        manifest_modified_at: TimestampMilliseconds(1_000_000_000_000),
        retention: Duration::from_secs(90 * 24 * 60 * 60),
        compacted_durable: true,
        manifest_readable: true,
        raw_table_entry_removed: true,
        earlier_stages_complete: true,
    }
}

#[test]
fn expired_processed_objects_are_deleted() {
    assert_eq!(base().decide(), RawDeletionDecision::Delete);
}

#[test]
fn retention_blocks_young_objects() {
    let mut input = base();
    input.processed_at = TimestampMilliseconds(1_999_999_999_000);
    assert_eq!(input.decide(), RawDeletionDecision::Retain);

    let mut input = base();
    input.manifest_modified_at = TimestampMilliseconds(1_999_999_999_000);
    assert_eq!(input.decide(), RawDeletionDecision::Retain);
}

#[test]
fn incomplete_or_unsafe_stages_block_deletion() {
    let mut input = base();
    input.compacted_durable = false;
    assert_eq!(input.decide(), RawDeletionDecision::Blocked);

    let mut input = base();
    input.raw_table_entry_removed = false;
    assert_eq!(input.decide(), RawDeletionDecision::Blocked);

    let mut input = base();
    input.earlier_stages_complete = false;
    assert_eq!(input.decide(), RawDeletionDecision::Blocked);
}

#[test]
fn unreadable_manifests_are_quarantined() {
    let mut input = base();
    input.manifest_readable = false;
    assert_eq!(input.decide(), RawDeletionDecision::Quarantine);
}
