use crate::{ArchiveKind, CompactionId};
use platform_types::TimestampMilliseconds;

fn digest() -> String {
    "0123456789abcdef".repeat(4)
}

#[test]
fn batch_ids_are_deterministic() {
    let start = TimestampMilliseconds(1_700_000_000_000);
    let first = CompactionId::for_batch(ArchiveKind::Events, start, &digest()).unwrap();
    let second = CompactionId::for_batch(ArchiveKind::Events, start, &digest()).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.to_string().len(), 36);
}

#[test]
fn batch_ids_change_with_kind_bucket_and_digest() {
    let start = TimestampMilliseconds(1_700_000_000_000);
    let first = CompactionId::for_batch(ArchiveKind::Events, start, &digest()).unwrap();

    assert_ne!(
        first,
        CompactionId::for_batch(ArchiveKind::Candles, start, &digest()).unwrap()
    );
    assert_ne!(
        first,
        CompactionId::for_batch(
            ArchiveKind::Events,
            TimestampMilliseconds(start.0 + 1),
            &digest()
        )
        .unwrap()
    );
    assert_ne!(
        first,
        CompactionId::for_batch(ArchiveKind::Events, start, &"f".repeat(64)).unwrap()
    );
}

#[test]
fn batch_ids_round_trip_through_text() {
    let id = CompactionId::for_batch(
        ArchiveKind::Candles,
        TimestampMilliseconds(1_700_000_000_000),
        &digest(),
    )
    .unwrap();

    assert_eq!(id.as_str().parse::<CompactionId>().unwrap(), id);
    assert_eq!(CompactionId::try_from(id.as_str()).unwrap(), id);
}

#[test]
fn batch_ids_reject_incomplete_digests() {
    let error = CompactionId::for_batch(
        ArchiveKind::Events,
        TimestampMilliseconds(1_700_000_000_000),
        "not-a-digest",
    )
    .unwrap_err();

    assert!(error.to_string().contains("full sha256 hex digest"));
}
