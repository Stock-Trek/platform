//! Gap markers and the candle windows they affect.

use super::AggregatorState;
use super::sequence::SequenceRange;
use platform_market_data::candle::CandleKey;
use platform_market_data::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

/// A source range that is known to be incomplete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownGap {
    pub exchange: CexId,
    pub stream: String,
    pub from_sequence: u64,
    pub to_sequence: u64,
    pub detected_at: TimestampMilliseconds,
}

impl KnownGap {
    pub fn new(gap: &GapMarker) -> Self {
        Self {
            exchange: gap.exchange,
            stream: gap.stream.clone(),
            from_sequence: gap.missing_from,
            to_sequence: gap.missing_to,
            detected_at: gap.detected_at,
        }
    }

    pub fn range(&self) -> SequenceRange {
        SequenceRange::new(self.from_sequence, self.to_sequence)
    }

    fn merge(&mut self, other: &Self) {
        self.from_sequence = self.from_sequence.min(other.from_sequence);
        self.to_sequence = self.to_sequence.max(other.to_sequence);
        self.detected_at = self.detected_at.max(other.detected_at);
    }
}

/// A candle window that may be missing trades because of a source gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncompleteWindow {
    pub key: CandleKey,
    pub window_start: TimestampMilliseconds,
    pub window_end: TimestampMilliseconds,
    pub stream: String,
    pub from_sequence: u64,
    pub to_sequence: u64,
    pub detected_at: TimestampMilliseconds,
}

impl IncompleteWindow {
    fn same_window(&self, other: &Self) -> bool {
        self.key == other.key && self.window_start == other.window_start
    }

    fn merge(&mut self, other: &Self) {
        self.from_sequence = self.from_sequence.min(other.from_sequence);
        self.to_sequence = self.to_sequence.max(other.to_sequence);
        self.window_end = self.window_end.max(other.window_end);
        self.detected_at = self.detected_at.max(other.detected_at);
    }
}

impl AggregatorState {
    /// Records a gap, merging overlapping or adjacent ranges for the same
    /// exchange stream.
    pub fn record_gap(&mut self, gap: &GapMarker) {
        let mut merged = KnownGap {
            exchange: gap.exchange,
            stream: gap.stream.clone(),
            from_sequence: gap.missing_from,
            to_sequence: gap.missing_to,
            detected_at: gap.detected_at,
        };
        self.known_gaps.retain(|existing| {
            if existing.exchange == merged.exchange
                && existing.stream == merged.stream
                && existing.range().overlaps_or_adjacent(&merged.range())
            {
                merged.merge(existing);
                false
            } else {
                true
            }
        });
        self.known_gaps.push(merged);
        self.known_gaps.sort_by(|left, right| {
            left.exchange
                .to_string()
                .cmp(&right.exchange.to_string())
                .then_with(|| left.stream.cmp(&right.stream))
                .then_with(|| left.from_sequence.cmp(&right.from_sequence))
        });
    }

    pub fn record_incomplete_window(&mut self, window: IncompleteWindow) {
        if let Some(existing) = self
            .incomplete_windows
            .iter_mut()
            .find(|existing| existing.same_window(&window))
        {
            existing.merge(&window);
        } else {
            self.incomplete_windows.push(window);
        }
        self.incomplete_windows.sort_by(|left, right| {
            left.key
                .subject_suffix()
                .cmp(&right.key.subject_suffix())
                .then_with(|| left.window_start.cmp(&right.window_start))
                .then_with(|| left.from_sequence.cmp(&right.from_sequence))
        });
    }

    /// Drops gap bookkeeping that can no longer affect an open candle.
    ///
    /// `cutoff` is the newest window end that has been closed. Candle
    /// completeness is copied onto the candle before this is called, so the
    /// records are no longer needed by live processing. Keeping only currently
    /// relevant entries prevents checkpoints from growing forever.
    pub fn prune_closed_windows(&mut self, cutoff: TimestampMilliseconds) {
        self.incomplete_windows.retain(|window| {
            window.window_end > cutoff
                || self.open_candles.get(&window.key).is_some_and(|candles| {
                    candles
                        .iter()
                        .any(|candle| candle.start_time() == window.window_start)
                })
        });
        self.known_gaps.retain(|gap| gap.detected_at > cutoff);
    }
}
