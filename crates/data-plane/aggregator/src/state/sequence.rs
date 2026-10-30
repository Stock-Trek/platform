//! Inclusive source sequence ranges.

use serde::{Deserialize, Serialize};

/// An inclusive range of source sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SequenceRange {
    pub from_sequence: u64,
    pub to_sequence: u64,
}

impl SequenceRange {
    pub fn new(from_sequence: u64, to_sequence: u64) -> Self {
        Self {
            from_sequence,
            to_sequence,
        }
    }

    pub fn contains(&self, sequence: u64) -> bool {
        self.from_sequence <= sequence && sequence <= self.to_sequence
    }

    pub fn overlaps_or_adjacent(&self, other: &Self) -> bool {
        self.from_sequence <= other.to_sequence.saturating_add(1)
            && other.from_sequence <= self.to_sequence.saturating_add(1)
    }
}
