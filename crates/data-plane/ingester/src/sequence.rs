#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceObservation {
    First,
    InOrder,
    Gap { missing_from: u64, missing_to: u64 },
    Duplicate,
    OutOfOrder,
}

#[derive(Debug, Default)]
pub struct SequenceTracker {
    /// First sequence that has not yet been published or covered by a gap
    /// marker.
    ///
    /// Stored as `u128` so the sequence immediately after `u64::MAX` is
    /// representable.
    next: Option<u128>,
}

impl SequenceTracker {
    /// Observes an inclusive range of update IDs without advancing the tracker.
    ///
    /// Exchanges such as Binance can pack many update IDs into one depth
    /// event. Continuity is checked against the first update ID in the range,
    /// not against `previous + 1` of the final ID.
    ///
    /// Call [`Self::commit`] only after the event has been published, so a
    /// failed publish remains visible as a gap to the next event.
    pub fn observe_range(&self, first: u64, last: u64) -> SequenceObservation {
        debug_assert!(first <= last, "sequence range must not be inverted");
        let first = first.min(last);

        let Some(next) = self.next else {
            return SequenceObservation::First;
        };

        let last = u128::from(last);
        if last < next {
            return if last + 1 == next {
                SequenceObservation::Duplicate
            } else {
                SequenceObservation::OutOfOrder
            };
        }

        let first = u128::from(first);
        if first > next {
            return SequenceObservation::Gap {
                missing_from: u64::try_from(next)
                    .expect("next sequence is within the observed range"),
                missing_to: u64::try_from(first - 1).expect("first is at most u64::MAX"),
            };
        }

        SequenceObservation::InOrder
    }

    /// Marks the inclusive range ending at `last` as successfully handled.
    pub fn commit(&mut self, last: u64) {
        self.next = Some(u128::from(last) + 1);
    }

    /// Records that an event was observed but could not be published.
    ///
    /// This establishes the event as missing when it is the first observation
    /// for the stream. Later observations already have an earlier boundary, so
    /// they keep their existing next sequence.
    pub fn record_failure(&mut self, first: u64) {
        if self.next.is_none() {
            self.next = Some(u128::from(first));
        }
    }
}

#[cfg(test)]
mod tests {
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
}
