//! The keyed candle processor that coordinates folding, deduplication, and
//! window expiry.

use super::{ProcessorConfig, ProcessorUpdate};
use crate::state::{AggregatorState, SequenceRange, stream_id};
use platform_errors::BpResult;
use platform_market_data::{
    candle::{CandleInterval, ClosedCandle},
    decoders::trade::{Trade, TradeDecoderFactory},
    event::{GapMarker, MarketEvent},
};
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

/// Pure keyed candle processor.
#[derive(Debug)]
pub struct CandleProcessor {
    pub(super) config: ProcessorConfig,
    pub(super) state: AggregatorState,
}

impl CandleProcessor {
    pub fn new(intervals: Vec<CandleInterval>) -> Self {
        Self::with_config(ProcessorConfig::with_intervals(intervals))
    }

    pub fn with_config(config: ProcessorConfig) -> Self {
        let state = AggregatorState::new_with_schema_version(config.state_schema_version);
        Self { config, state }
    }

    pub fn from_state(config: ProcessorConfig, mut state: AggregatorState) -> BpResult<Self> {
        state.ensure_schema_compatible_with(config.state_schema_version)?;
        state.prune_stream_dedup();
        state.prune_closed_candles_to_cap(config.max_closed_candles_per_key);
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

    /// Closes windows that are already expired at the current event-time
    /// watermark without moving event time forward.
    ///
    /// This is used by idle ticks. Wall-clock time is not event time: advancing
    /// the watermark from an idle tick would close windows for which later
    /// backfilled trades are still expected.
    pub fn flush_expired(&mut self) -> ProcessorUpdate {
        let mut update = ProcessorUpdate::default();
        self.close_expired(&mut update);
        update
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
