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

    pub fn observe(&mut self, sequence: u64) -> SequenceObservation {
        let Some(last) = self.last else {
            self.last = Some(sequence);
            return SequenceObservation::First;
        };

        if sequence == last {
            return SequenceObservation::Duplicate;
        }
        if sequence < last {
            return SequenceObservation::OutOfOrder;
        }

        self.last = Some(sequence);
        match last.checked_add(1) {
            Some(expected) if sequence == expected => SequenceObservation::InOrder,
            Some(expected) => SequenceObservation::Gap {
                missing_from: expected,
                missing_to: sequence - 1,
            },
            None => SequenceObservation::InOrder,
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
}
