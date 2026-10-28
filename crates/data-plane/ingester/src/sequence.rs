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
    last: Option<u64>,
}

impl SequenceTracker {
    #[cfg(test)]
    pub fn last(&self) -> Option<u64> {
        self.last
    }

    #[cfg(test)]
    pub fn observe(&mut self, sequence: u64) -> SequenceObservation {
        self.observe_range(sequence, sequence)
    }

    /// Observes an inclusive range of update IDs.
    ///
    /// Exchanges such as Binance can pack many update IDs into one depth
    /// event. Continuity is checked against the first update ID in the range,
    /// not against `previous + 1` of the final ID.
    pub fn observe_range(&mut self, first: u64, last: u64) -> SequenceObservation {
        debug_assert!(first <= last, "sequence range must not be inverted");
        let first = first.min(last);

        let Some(previous) = self.last else {
            self.last = Some(last);
            return SequenceObservation::First;
        };

        if last == previous {
            return SequenceObservation::Duplicate;
        }
        if last < previous {
            return SequenceObservation::OutOfOrder;
        }

        self.last = Some(last);
        match previous.checked_add(1) {
            Some(expected) if first > expected => SequenceObservation::Gap {
                missing_from: expected,
                missing_to: first - 1,
            },
            _ => SequenceObservation::InOrder,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sequence_establishes_the_baseline() {
        let mut tracker = SequenceTracker::default();

        assert_eq!(tracker.observe(100), SequenceObservation::First);
        assert_eq!(tracker.last(), Some(100));
    }

    #[test]
    fn consecutive_sequences_are_in_order() {
        let mut tracker = SequenceTracker::default();

        tracker.observe(100);
        assert_eq!(tracker.observe(101), SequenceObservation::InOrder);
        assert_eq!(tracker.observe(102), SequenceObservation::InOrder);
    }

    #[test]
    fn gaps_report_the_missing_inclusive_range() {
        let mut tracker = SequenceTracker::default();

        tracker.observe(100);
        assert_eq!(
            tracker.observe(104),
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

        tracker.observe(100);
        assert_eq!(tracker.observe(100), SequenceObservation::Duplicate);
        assert_eq!(tracker.observe(99), SequenceObservation::OutOfOrder);
        assert_eq!(tracker.last(), Some(100));
    }

    #[test]
    fn maximum_sequence_does_not_overflow() {
        let mut tracker = SequenceTracker::default();

        tracker.observe(u64::MAX);
        assert_eq!(tracker.observe(u64::MAX), SequenceObservation::Duplicate);
    }

    #[test]
    fn ranges_spanning_many_update_ids_are_in_order() {
        let mut tracker = SequenceTracker::default();

        assert_eq!(tracker.observe_range(100, 105), SequenceObservation::First);
        assert_eq!(
            tracker.observe_range(106, 120),
            SequenceObservation::InOrder
        );
        assert_eq!(tracker.last(), Some(120));
    }

    #[test]
    fn range_gaps_are_measured_from_the_first_update_id() {
        let mut tracker = SequenceTracker::default();

        tracker.observe_range(100, 105);
        assert_eq!(
            tracker.observe_range(110, 120),
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

        tracker.observe_range(100, 105);
        assert_eq!(
            tracker.observe_range(103, 108),
            SequenceObservation::InOrder
        );
        assert_eq!(tracker.last(), Some(108));
    }

    #[test]
    fn ranges_that_end_at_the_previous_sequence_are_duplicates() {
        let mut tracker = SequenceTracker::default();

        tracker.observe_range(100, 105);
        assert_eq!(
            tracker.observe_range(103, 105),
            SequenceObservation::Duplicate
        );
        assert_eq!(tracker.last(), Some(105));
    }
}
