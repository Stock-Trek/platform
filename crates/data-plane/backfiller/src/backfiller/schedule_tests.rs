use crate::gap_retry_store::{MAX_RETRY_DELAY, PendingGap};
use platform_market_data::GapMarker;
use platform_types::TimestampMilliseconds;

#[test]
fn finds_contiguous_unresolved_ranges() {
    assert_eq!(
        PendingGap::missing_ranges(10, 15, [10, 11, 12, 13, 14, 15]),
        vec![]
    );
    assert_eq!(
        PendingGap::missing_ranges(10, 15, [10, 12, 15]),
        vec![(11, 11), (13, 14)]
    );
    assert_eq!(PendingGap::missing_ranges(10, 15, []), vec![(10, 15)]);
    assert_eq!(PendingGap::missing_ranges(10, 10, [10]), vec![]);
}

#[test]
fn retry_attempts_widen_the_rest_window() {
    let widened = PendingGap::widened_gap(&GapMarker::gap(10_000, 20_000), 2);

    assert_eq!(widened.missing_from, 8_000);
    assert_eq!(widened.missing_to, 22_000);
    assert_eq!(widened.previous_sequence, 7_999);
    assert_eq!(widened.observed_sequence, 22_001);
}

#[test]
fn deferred_retry_attempts_are_bounded() {
    let now = TimestampMilliseconds(1_000);
    let gap = PendingGap::from_range(&GapMarker::gap(10, 20), 10, 20, 100, now, None);

    let deferred = gap.deferred(now);

    assert_eq!(deferred.attempts, 101);
    let expected_delay = i64::try_from(MAX_RETRY_DELAY.as_millis()).unwrap();
    assert_eq!(
        deferred.next_attempt_at,
        TimestampMilliseconds(now.0 + expected_delay)
    );
}
