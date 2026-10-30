//! Deterministic candle aggregation.
//!
//! [`CandleProcessor`] is a pure fold over keyed state: it never reads the wall
//! clock and performs no I/O. The worker persists the updated state and emits
//! the returned outputs, which makes replaying an event log reproducible.
//!
//! The processor is split into focused modules: `candle` folds trades into
//! open or closed candles, `dedup` rejects already-seen events, and `window`
//! closes expired windows and marks windows affected by source gaps.

mod candle;
mod dedup;
mod window;

#[cfg(test)]
mod tests;

use crate::state::{AggregatorState, IncompleteWindow, SequenceRange, stream_id};
use platform_errors::BpResult;
use platform_market_data::{
    candle::{Candle, CandleInterval, ClosedCandle},
    decoders::trade::{Trade, TradeDecoderFactory},
    event::{GapMarker, MarketEvent},
};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::HashSet;
use stock_trek_types::cex::cex_id::CexId;

/// Interval and lateness settings shared by live processing and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorConfig {
    pub intervals: Vec<CandleInterval>,
    pub allowed_lateness: DurationMilliseconds,
    /// Schema version new state is stamped with and restored state must match.
    pub state_schema_version: u32,
}

impl ProcessorConfig {
    pub fn new(intervals: Vec<CandleInterval>, allowed_lateness: DurationMilliseconds) -> Self {
        Self {
            intervals,
            allowed_lateness,
            state_schema_version: crate::state::STATE_SCHEMA_VERSION,
        }
    }

    pub fn with_intervals(intervals: Vec<CandleInterval>) -> Self {
        Self::new(intervals, DurationMilliseconds::ZERO)
    }

    pub fn with_state_schema_version(mut self, state_schema_version: u32) -> Self {
        self.state_schema_version = state_schema_version;
        self
    }
}

/// Outputs produced by applying one input to the keyed state.
#[derive(Debug, Default, Clone)]
pub struct ProcessorUpdate {
    pub open: Vec<Candle>,
    pub closed: Vec<ClosedCandle>,
    pub incomplete_windows: Vec<IncompleteWindow>,
    /// Events dropped because their `(exchange, stream, sequence)` was seen.
    pub duplicates: usize,
    /// Trades that arrived after their candle's allowed-lateness window closed
    /// and were applied as closed-candle corrections.
    pub late_trades: usize,
}

impl ProcessorUpdate {
    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
            && self.closed.is_empty()
            && self.incomplete_windows.is_empty()
            && self.duplicates == 0
            && self.late_trades == 0
    }

    pub fn has_outputs(&self) -> bool {
        !self.open.is_empty() || !self.closed.is_empty()
    }

    /// Keeps only the last open-candle value for each [`platform_market_data::candle::CandleKey`].
    ///
    /// Open candles are a derived projection keyed only by candle identity, so
    /// intermediate values produced while folding multiple trades are not
    /// observable. Coalescing them here avoids cloning and writing each
    /// intermediate value to the shared KV bucket.
    pub fn coalesce_open(&mut self) {
        if self.open.len() < 2 {
            return;
        }
        let mut seen = HashSet::with_capacity(self.open.len());
        self.open.reverse();
        self.open.retain(|candle| seen.insert(candle.key.clone()));
        self.open.reverse();
    }
}

/// Pure keyed candle processor.
#[derive(Debug)]
pub struct CandleProcessor {
    config: ProcessorConfig,
    state: AggregatorState,
}

impl CandleProcessor {
    pub fn new(intervals: Vec<CandleInterval>) -> Self {
        Self::with_config(ProcessorConfig::with_intervals(intervals))
    }

    pub fn with_config(config: ProcessorConfig) -> Self {
        let state = AggregatorState::new_with_schema_version(config.state_schema_version);
        Self { config, state }
    }

    pub fn from_state(config: ProcessorConfig, state: AggregatorState) -> BpResult<Self> {
        state.ensure_schema_compatible_with(config.state_schema_version)?;
        Ok(Self { config, state })
    }

    pub fn config(&self) -> &ProcessorConfig {
        &self.config
    }

    pub fn state(&self) -> &AggregatorState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut AggregatorState {
        &mut self.state
    }

    pub fn into_state(self) -> AggregatorState {
        self.state
    }

    /// Applies one decoded market event and closes any expired candle windows.
    ///
    /// The event is deduplicated by `(exchange, stream, sequence)`. The
    /// watermark is advanced from the event's exchange timestamp, bounded by
    /// when the event was received, before trades are folded in. Windows close
    /// against their own stream's watermark plus the configured allowed
    /// lateness.
    pub fn apply_event(&mut self, event: &MarketEvent, trades: &[Trade]) -> ProcessorUpdate {
        let mut update = ProcessorUpdate::default();
        let timestamp = event.exchange_timestamp.unwrap_or(event.received_at);
        let watermark = timestamp.min(event.received_at);

        if !self.accept_event(event, &mut update) {
            return update;
        }

        self.state
            .advance_stream_watermark(event.exchange, &event.stream, watermark);
        self.state.advance_watermark(watermark);

        for trade in trades {
            self.apply_trade_inner(trade, Some(&event.stream), &mut update);
        }
        self.close_expired(&mut update);
        update.coalesce_open();
        update
    }

    /// Applies one trade directly. This is used by recovery and unit tests where
    /// there is no enclosing market event.
    pub fn apply_trade(&mut self, trade: &Trade) -> ProcessorUpdate {
        let mut update = ProcessorUpdate::default();
        self.state.advance_watermark(trade.timestamp);
        self.apply_trade_inner(trade, None, &mut update);
        self.close_expired(&mut update);
        update.coalesce_open();
        update
    }

    /// Records a gap as an incompleteness signal for the affected stream and
    /// candle windows, advances the event-time watermark, and closes windows
    /// whose allowed-lateness window has elapsed.
    pub fn apply_gap(&mut self, gap: &GapMarker) -> ProcessorUpdate {
        let mut update = ProcessorUpdate::default();
        let previous_stream_watermark = self.state.stream_watermark(gap.exchange, &gap.stream);
        let trading_pair =
            TradeDecoderFactory::trading_pair_for_stream(gap.exchange, &gap.stream).ok();

        self.state
            .advance_stream_watermark(gap.exchange, &gap.stream, gap.detected_at);
        self.state.advance_watermark(gap.detected_at);
        self.state.record_gap(gap);
        self.mark_affected_windows(gap, trading_pair, previous_stream_watermark, &mut update);
        self.close_expired(&mut update);
        update.coalesce_open();
        update
    }

    /// Advances the event-time watermark and closes every candle whose window
    /// ended at or before it, accounting for allowed lateness.
    pub fn flush_closed_at_or_before(
        &mut self,
        timestamp: TimestampMilliseconds,
    ) -> Vec<ClosedCandle> {
        self.state.advance_watermark(timestamp);
        let mut update = ProcessorUpdate::default();
        self.close_expired_at(Some(timestamp), &mut update);
        update.closed
    }

    /// Returns the sequences seen for one exchange stream.
    pub fn seen_sequences(&self, exchange: CexId, stream: &str) -> Vec<SequenceRange> {
        self.state
            .streams
            .get(&stream_id(exchange, stream))
            .map(|progress| progress.seen_sequences.clone())
            .unwrap_or_default()
    }
}
