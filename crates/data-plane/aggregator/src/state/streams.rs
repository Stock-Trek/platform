//! Per-stream event-time progress and deduplication.

use super::AggregatorState;
use super::sequence::SequenceRange;
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

/// Identity of an element in an exchange stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeduplicationKey {
    pub exchange: CexId,
    pub stream: String,
    pub sequence: u64,
}

impl DeduplicationKey {
    pub fn new(exchange: CexId, stream: impl Into<String>, sequence: u64) -> Self {
        Self {
            exchange,
            stream: stream.into(),
            sequence,
        }
    }

    pub fn stream_id(&self) -> String {
        stream_id(self.exchange, &self.stream)
    }
}

/// Stable key for the progress of one exchange stream.
pub fn stream_id(exchange: CexId, stream: &str) -> String {
    format!("{exchange}.{stream}")
}

/// Per-stream event-time progress and deduplication ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamProgress {
    pub watermark: Option<TimestampMilliseconds>,
    pub last_sequence: Option<u64>,
    /// Inclusive sequence ranges already applied for this stream.
    pub seen_sequences: Vec<SequenceRange>,
}

impl StreamProgress {
    pub fn is_duplicate(&self, sequence: u64) -> bool {
        self.seen_sequences
            .iter()
            .any(|range| range.contains(sequence))
    }

    /// Records a sequence and returns `false` when it had already been seen.
    pub fn observe_sequence(&mut self, sequence: u64) -> bool {
        if self.is_duplicate(sequence) {
            return false;
        }
        self.insert_sequence(sequence);
        self.last_sequence = Some(match self.last_sequence {
            Some(current) => current.max(sequence),
            None => sequence,
        });
        true
    }

    pub fn advance_watermark(&mut self, timestamp: TimestampMilliseconds) -> bool {
        match self.watermark {
            Some(current) if current >= timestamp => false,
            _ => {
                self.watermark = Some(timestamp);
                true
            }
        }
    }

    fn insert_sequence(&mut self, sequence: u64) {
        let mut merged = SequenceRange::new(sequence, sequence);
        self.seen_sequences.retain(|existing| {
            if existing.overlaps_or_adjacent(&merged) {
                merged.from_sequence = merged.from_sequence.min(existing.from_sequence);
                merged.to_sequence = merged.to_sequence.max(existing.to_sequence);
                false
            } else {
                true
            }
        });
        self.seen_sequences.push(merged);
        self.seen_sequences.sort();
    }
}

impl AggregatorState {
    pub fn stream_watermark(&self, exchange: CexId, stream: &str) -> Option<TimestampMilliseconds> {
        self.streams
            .get(&stream_id(exchange, stream))
            .and_then(|progress| progress.watermark)
    }

    pub fn advance_stream_watermark(
        &mut self,
        exchange: CexId,
        stream: &str,
        timestamp: TimestampMilliseconds,
    ) -> bool {
        self.streams
            .entry(stream_id(exchange, stream))
            .or_default()
            .advance_watermark(timestamp)
    }

    pub fn is_duplicate(&self, key: &DeduplicationKey) -> bool {
        self.streams
            .get(&key.stream_id())
            .is_some_and(|progress| progress.is_duplicate(key.sequence))
    }

    /// Marks a key as seen. Returns `false` when it was already seen.
    pub fn observe_sequence(&mut self, key: &DeduplicationKey) -> bool {
        self.streams
            .entry(key.stream_id())
            .or_default()
            .observe_sequence(key.sequence)
    }
}
