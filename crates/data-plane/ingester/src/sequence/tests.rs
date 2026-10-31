use async_trait::async_trait;
use std::{collections::HashMap, sync::Mutex};

/// Sequence state kept in process memory.
#[derive(Debug, Default)]
pub struct InMemorySequenceStore {
    trackers: Mutex<HashMap<String, SequenceTracker>>,
}

impl InMemorySequenceStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SequenceStore for InMemorySequenceStore {
    async fn load(&self, key: &str) -> BpResult<Option<SequenceTracker>> {
        Ok(self
            .trackers
            .lock()
            .expect("sequence store lock")
            .get(key)
            .cloned())
    }

    async fn save(&self, key: &str, tracker: &SequenceTracker) -> BpResult<()> {
        self.trackers
            .lock()
            .expect("sequence store lock")
            .insert(key.to_string(), tracker.clone());
        Ok(())
    }
}

use super::*;

trait SequenceTrackerTestExt {
    fn last(&self) -> Option<u64>;
}

impl SequenceTrackerTestExt for SequenceTracker {
    fn last(&self) -> Option<u64> {
        self.next
            .and_then(|next| next.checked_sub(1))
            .and_then(|last| u64::try_from(last).ok())
    }
}

fn observe(tracker: &mut SequenceTracker, first: u64, last: u64) -> SequenceObservation {
    let observation = tracker.observe_range(first, last);
    if !matches!(
        observation,
        SequenceObservation::Duplicate | SequenceObservation::OutOfOrder
    ) {
        tracker.commit(last);
    }
    observation
}

#[test]
fn observation_does_not_advance_until_committed() {
    let mut tracker = SequenceTracker::default();

    assert_eq!(tracker.observe_range(100, 100), SequenceObservation::First);
    assert_eq!(tracker.last(), None);
    assert_eq!(tracker.observe_range(100, 100), SequenceObservation::First);

    tracker.commit(100);
    assert_eq!(tracker.last(), Some(100));
}

#[test]
fn first_sequence_establishes_the_baseline() {
    let mut tracker = SequenceTracker::default();

    assert_eq!(observe(&mut tracker, 100, 100), SequenceObservation::First);
    assert_eq!(tracker.last(), Some(100));
}

#[test]
fn consecutive_sequences_are_in_order() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 100);
    assert_eq!(
        observe(&mut tracker, 101, 101),
        SequenceObservation::InOrder
    );
    assert_eq!(
        observe(&mut tracker, 102, 102),
        SequenceObservation::InOrder
    );
}

#[test]
fn gaps_report_the_missing_inclusive_range() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 100);
    assert_eq!(
        observe(&mut tracker, 104, 104),
        SequenceObservation::Gap {
            missing_from: 101,
            missing_to: 103,
        }
    );
    assert_eq!(tracker.last(), Some(104));
}

#[test]
fn duplicate_and_older_sequences_do_not_advance_the_tracker() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 100);
    assert_eq!(
        observe(&mut tracker, 100, 100),
        SequenceObservation::Duplicate
    );
    assert_eq!(
        observe(&mut tracker, 99, 99),
        SequenceObservation::OutOfOrder
    );
    assert_eq!(tracker.last(), Some(100));
}

#[test]
fn maximum_sequence_does_not_overflow() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, u64::MAX, u64::MAX);
    assert_eq!(
        observe(&mut tracker, u64::MAX, u64::MAX),
        SequenceObservation::Duplicate
    );
}

#[test]
fn serialized_state_preserves_the_sequence_after_u64_max() {
    let mut tracker = SequenceTracker::default();
    tracker.commit(u64::MAX);

    let encoded = serde_json::to_string(&tracker).unwrap();
    let decoded: SequenceTracker = serde_json::from_str(&encoded).unwrap();

    assert_eq!(
        decoded.observe_range(u64::MAX, u64::MAX),
        SequenceObservation::Duplicate
    );
}

#[tokio::test]
async fn in_memory_store_round_trips_trackers() {
    let store = InMemorySequenceStore::new();
    let mut tracker = SequenceTracker::default();
    tracker.commit(41);

    store.save("binance.btcusdt.trade", &tracker).await.unwrap();

    assert_eq!(
        store.load("binance.btcusdt.trade").await.unwrap(),
        Some(tracker)
    );
    assert_eq!(store.load("binance.ethusdt.trade").await.unwrap(), None);
}

#[test]
fn sequence_bucket_is_derived_from_the_subject_prefix() {
    assert_eq!(
        NatsSequenceStore::bucket_name("market"),
        "market_ingester-sequences"
    );
}

#[test]
fn ranges_spanning_many_update_ids_are_in_order() {
    let mut tracker = SequenceTracker::default();

    assert_eq!(observe(&mut tracker, 100, 105), SequenceObservation::First);
    assert_eq!(
        observe(&mut tracker, 106, 120),
        SequenceObservation::InOrder
    );
    assert_eq!(tracker.last(), Some(120));
}

#[test]
fn range_gaps_are_measured_from_the_first_update_id() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 105);
    assert_eq!(
        observe(&mut tracker, 110, 120),
        SequenceObservation::Gap {
            missing_from: 106,
            missing_to: 109,
        }
    );
    assert_eq!(tracker.last(), Some(120));
}

#[test]
fn overlapping_ranges_advance_the_final_sequence() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 105);
    assert_eq!(
        observe(&mut tracker, 103, 108),
        SequenceObservation::InOrder
    );
    assert_eq!(tracker.last(), Some(108));
}

#[test]
fn ranges_that_end_at_the_previous_sequence_are_duplicates() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 105);
    assert_eq!(
        observe(&mut tracker, 103, 105),
        SequenceObservation::Duplicate
    );
    assert_eq!(tracker.last(), Some(105));
}

#[test]
fn a_failed_first_event_is_a_gap_for_the_next_event() {
    let mut tracker = SequenceTracker::default();

    assert_eq!(tracker.observe_range(10, 10), SequenceObservation::First);
    tracker.record_failure(10);

    assert_eq!(
        tracker.observe_range(11, 11),
        SequenceObservation::Gap {
            missing_from: 10,
            missing_to: 10,
        }
    );
}

#[test]
fn a_failed_publish_is_a_gap_for_the_next_event() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 100);
    assert_eq!(
        tracker.observe_range(101, 101),
        SequenceObservation::InOrder
    );
    tracker.record_failure(101);

    assert_eq!(
        tracker.observe_range(102, 102),
        SequenceObservation::Gap {
            missing_from: 101,
            missing_to: 101,
        }
    );
}

#[test]
fn a_failed_overlapping_publish_does_not_move_the_boundary_backwards() {
    let mut tracker = SequenceTracker::default();

    observe(&mut tracker, 100, 105);
    assert_eq!(
        tracker.observe_range(103, 108),
        SequenceObservation::InOrder
    );
    tracker.record_failure(103);

    assert_eq!(
        tracker.observe_range(109, 109),
        SequenceObservation::Gap {
            missing_from: 106,
            missing_to: 108,
        }
    );
}
