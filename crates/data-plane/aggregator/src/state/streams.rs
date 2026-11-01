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
        StreamProgress::stream_id(self.exchange, &self.stream)
    }
}

/// Maximum number of disjoint sequence ranges retained per stream.
///
/// Exchange sequence numbers can be sparse or permanently missing, so a stream
/// can otherwise accumulate one range per event forever. Retaining only the
/// most recent ranges bounds checkpoint size. Older sequences beyond this
/// horizon are no longer recognized as duplicates.
pub const MAX_SEEN_SEQUENCE_RANGES: usize = 1_024;

/// Per-stream event-time progress and deduplication ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamProgress {
    pub watermark: Option<TimestampMilliseconds>,
    pub last_sequence: Option<u64>,
    /// Inclusive sequence ranges already applied for this stream.
    ///
    /// Bounded to [`MAX_SEEN_SEQUENCE_RANGES`] entries so sparse streams cannot
    /// grow checkpoints without limit.
    pub seen_sequences: Vec<SequenceRange>,
}

impl StreamProgress {
    /// Stable key for the progress of one exchange stream.
    pub fn stream_id(exchange: CexId, stream: &str) -> String {
        format!("{exchange}.{stream}")
    }

    pub fn is_duplicate(&self, sequence: u64) -> bool {
        self.seen_sequences
            .iter()
            .any(|range| range.contains(sequence))
    }

    /// Returns whether every sequence in the inclusive interval has been seen.
    pub fn covers_sequence_range(&self, from_sequence: u64, to_sequence: u64) -> bool {
        self.seen_sequences
            .iter()
            .any(|range| range.covers(from_sequence, to_sequence))
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

    /// Drops the oldest ranges when the retained set exceeds the per-stream cap.
    pub fn prune_seen_sequences(&mut self) {
        if self.seen_sequences.len() > MAX_SEEN_SEQUENCE_RANGES {
            let excess = self.seen_sequences.len() - MAX_SEEN_SEQUENCE_RANGES;
            self.seen_sequences.drain(..excess);
        }
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
        self.prune_seen_sequences();
    }
}

impl AggregatorState {
    pub fn stream_watermark(&self, exchange: CexId, stream: &str) -> Option<TimestampMilliseconds> {
        self.streams
            .get(&StreamProgress::stream_id(exchange, stream))
            .and_then(|progress| progress.watermark)
    }

    pub fn advance_stream_watermark(
        &mut self,
        exchange: CexId,
        stream: &str,
        timestamp: TimestampMilliseconds,
    ) -> bool {
        self.streams
            .entry(StreamProgress::stream_id(exchange, stream))
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

    /// Applies the per-stream deduplication bound to restored state.
    pub fn prune_stream_dedup(&mut self) {
        for progress in self.streams.values_mut() {
            progress.prune_seen_sequences();
        }
    }
}
