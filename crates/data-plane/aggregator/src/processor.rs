//! Deterministic candle aggregation.
//!
//! [`CandleProcessor`] is a pure fold over keyed state: it never reads the wall
//! clock and performs no I/O. The worker persists the updated state and emits
//! the returned outputs, which makes replaying an event log reproducible.

use crate::state::{AggregatorState, DeduplicationKey, IncompleteWindow, SequenceRange, stream_id};
use platform_errors::BpResult;
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    decoders::trade::{Trade, TradeDecoderFactory},
    event::{GapMarker, MarketEvent},
};
use platform_metrics::descriptions::{DROP_REASON_DUPLICATE, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::HashSet;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

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

    /// Keeps only the last open-candle value for each [`CandleKey`].
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

        if let Some(sequence) = event.sequence {
            let key = DeduplicationKey::new(event.exchange, &event.stream, sequence);
            if !self.state.observe_sequence(&key) {
                update.duplicates += 1;
                MarketDataMetrics::record_dropped_event(STAGE_AGGREGATOR, DROP_REASON_DUPLICATE);
                return update;
            }
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

    fn apply_trade_inner(
        &mut self,
        trade: &Trade,
        stream: Option<&str>,
        update: &mut ProcessorUpdate,
    ) {
        for index in 0..self.config.intervals.len() {
            let interval = self.config.intervals[index];
            let key = CandleKey::new(trade.exchange, trade.trading_pair, interval);
            let candle_start = interval.start_for(trade.timestamp);
            let candle_end = interval.end_for(trade.timestamp);
            let watermark = self.watermark_for_trade(trade, stream);
            let window_is_closed = watermark
                .is_some_and(|watermark| candle_end + self.config.allowed_lateness <= watermark);

            if self.state.closed_candles.get(&key).is_some_and(|candles| {
                candles
                    .binary_search_by_key(&candle_start.0, |candle| candle.start_time().0)
                    .is_ok()
            }) {
                self.apply_late_trade(trade, &key, candle_start, update);
                continue;
            }

            if window_is_closed {
                self.open_closed_correction(trade, interval, update);
                continue;
            }

            let candles = self.state.open_candles.entry(key).or_default();
            let position = match candles
                .binary_search_by_key(&candle_start.0, |candle| candle.start_time().0)
            {
                Ok(position) => {
                    if let Some(candle) = candles.get_mut(position) {
                        candle.apply_trade(trade);
                    }
                    position
                }
                Err(position) => {
                    candles.insert(position, Candle::open(trade, interval));
                    position
                }
            };
            if position + 1 == candles.len()
                && let Some(candle) = candles.get(position)
            {
                update.open.push(candle.clone());
            }
        }
    }

    fn apply_late_trade(
        &mut self,
        trade: &Trade,
        key: &CandleKey,
        candle_start: TimestampMilliseconds,
        update: &mut ProcessorUpdate,
    ) {
        let close_watermark = self.state.watermark.unwrap_or(trade.timestamp);
        let source_offset = self.state.last_processed_offset().unwrap_or_default();
        let state_epoch = self.state.state_epoch;
        let Some(candles) = self.state.closed_candles.get_mut(key) else {
            return;
        };
        let Ok(position) =
            candles.binary_search_by_key(&candle_start.0, |candle| candle.start_time().0)
        else {
            return;
        };
        let Some(candle) = candles.get_mut(position) else {
            return;
        };
        candle.apply_trade(trade);
        candle.close();
        update.late_trades += 1;
        update.closed.push(ClosedCandle::new(
            candle.clone(),
            close_watermark,
            source_offset,
            state_epoch,
        ));
    }

    fn open_closed_correction(
        &mut self,
        trade: &Trade,
        interval: CandleInterval,
        update: &mut ProcessorUpdate,
    ) {
        let close_watermark = self.state.watermark.unwrap_or(trade.timestamp);
        let source_offset = self.state.last_processed_offset().unwrap_or_default();
        let state_epoch = self.state.state_epoch;
        let mut candle = Candle::open(trade, interval);
        candle.close();
        self.state.retain_closed_candle(candle.clone());
        update.late_trades += 1;
        update.closed.push(ClosedCandle::new(
            candle,
            close_watermark,
            source_offset,
            state_epoch,
        ));
    }

    fn watermark_for_trade(
        &self,
        trade: &Trade,
        stream: Option<&str>,
    ) -> Option<TimestampMilliseconds> {
        if let Some(stream) = stream {
            return self.state.stream_watermark(trade.exchange, stream);
        }
        self.watermark_for_pair(trade.exchange, trade.trading_pair)
            .or_else(|| self.global_watermark_fallback())
    }

    fn global_watermark_fallback(&self) -> Option<TimestampMilliseconds> {
        if self.state.streams.is_empty() {
            self.state.watermark
        } else {
            None
        }
    }

    fn watermark_for_pair(
        &self,
        exchange: CexId,
        trading_pair: TradingPair,
    ) -> Option<TimestampMilliseconds> {
        let prefix = format!("{exchange}.");
        self.state
            .streams
            .iter()
            .filter_map(|(id, progress)| {
                let stream = id.strip_prefix(&prefix)?;
                let pair = TradeDecoderFactory::trading_pair_for_stream(exchange, stream).ok()?;
                (pair == trading_pair)
                    .then_some(progress.watermark)
                    .flatten()
            })
            .max()
    }

    fn close_expired(&mut self, update: &mut ProcessorUpdate) {
        self.close_expired_at(None, update);
    }

    fn close_expired_at(
        &mut self,
        explicit_watermark: Option<TimestampMilliseconds>,
        update: &mut ProcessorUpdate,
    ) {
        if self.state.watermark.is_none() {
            return;
        }
        let source_offset = self.state.last_processed_offset().unwrap_or_default();
        let state_epoch = self.state.state_epoch;
        let mut expired = Vec::new();
        for (key, candles) in &self.state.open_candles {
            let Some(watermark) = explicit_watermark.or_else(|| {
                self.watermark_for_pair(key.exchange, key.trading_pair)
                    .or_else(|| self.global_watermark_fallback())
            }) else {
                continue;
            };
            let cutoff = watermark - self.config.allowed_lateness;
            for candle in candles {
                if candle.end_time() <= cutoff {
                    expired.push((key.clone(), candle.start_time().0, watermark));
                }
            }
        }
        expired.sort_by(|(left_key, left_start, _), (right_key, right_start, _)| {
            left_key
                .subject_suffix()
                .cmp(&right_key.subject_suffix())
                .then_with(|| left_start.cmp(right_start))
        });

        for (key, start, watermark) in expired {
            let (mut candle, empty) = {
                let Some(candles) = self.state.open_candles.get_mut(&key) else {
                    continue;
                };
                let Ok(position) =
                    candles.binary_search_by_key(&start, |candle| candle.start_time().0)
                else {
                    continue;
                };
                let candle = candles.remove(position);
                let empty = candles.is_empty();
                (candle, empty)
            };
            if empty {
                self.state.open_candles.remove(&key);
            }
            candle.close();
            self.state.retain_closed_candle(candle.clone());
            update.closed.push(ClosedCandle::new(
                candle,
                watermark,
                source_offset,
                state_epoch,
            ));
        }
        if let Some(watermark) = self.state.watermark {
            let cutoff = watermark - self.config.allowed_lateness;
            self.state.prune_closed_windows(cutoff);
        }
    }

    fn mark_affected_windows(
        &mut self,
        gap: &GapMarker,
        trading_pair: Option<TradingPair>,
        previous_stream_watermark: Option<TimestampMilliseconds>,
        update: &mut ProcessorUpdate,
    ) {
        let mut seen = std::collections::HashSet::new();
        let mut affected = self
            .state
            .open_candles
            .iter()
            .chain(self.state.closed_candles.iter())
            .filter(|(key, _)| {
                key.exchange == gap.exchange
                    && trading_pair.is_none_or(|pair| key.trading_pair == pair)
            })
            .flat_map(|(key, candles)| {
                candles
                    .iter()
                    .filter(|candle| {
                        if candle.start_time() > gap.detected_at {
                            return false;
                        }
                        match previous_stream_watermark {
                            Some(previous) => {
                                candle.end_time() + self.config.allowed_lateness >= previous
                                    || gap.detected_at < candle.end_time()
                            }
                            None => true,
                        }
                    })
                    .map(move |candle| IncompleteWindow {
                        key: key.clone(),
                        window_start: candle.start_time(),
                        window_end: candle.end_time(),
                        stream: gap.stream.clone(),
                        from_sequence: gap.missing_from,
                        to_sequence: gap.missing_to,
                        detected_at: gap.detected_at,
                    })
            })
            .filter(|window| seen.insert((window.key.clone(), window.window_start)))
            .collect::<Vec<_>>();
        affected.sort_by(|left, right| {
            left.key
                .subject_suffix()
                .cmp(&right.key.subject_suffix())
                .then_with(|| left.window_start.cmp(&right.window_start))
        });

        let close_watermark = self.state.watermark.unwrap_or(gap.detected_at);
        let source_offset = self.state.last_processed_offset().unwrap_or_default();
        let state_epoch = self.state.state_epoch;
        for window in affected {
            if let Some(candle) = self
                .state
                .open_candles
                .get_mut(&window.key)
                .and_then(|candles| {
                    candles
                        .iter_mut()
                        .find(|candle| candle.start_time() == window.window_start)
                })
            {
                candle.mark_incomplete();
                update.open.push(candle.clone());
            }
            if let Some(candle) =
                self.state
                    .closed_candles
                    .get_mut(&window.key)
                    .and_then(|candles| {
                        candles
                            .iter_mut()
                            .find(|candle| candle.start_time() == window.window_start)
                    })
            {
                candle.mark_incomplete();
                candle.close();
                update.closed.push(ClosedCandle::new(
                    candle.clone(),
                    close_watermark,
                    source_offset,
                    state_epoch,
                ));
            }
            self.state.record_incomplete_window(window.clone());
            update.incomplete_windows.push(window);
        }
    }

    /// Returns the sequences seen for one exchange stream.
    pub fn seen_sequences(
        &self,
        exchange: stock_trek_types::cex::cex_id::CexId,
        stream: &str,
    ) -> Vec<SequenceRange> {
        self.state
            .streams
            .get(&stream_id(exchange, stream))
            .map(|progress| progress.seen_sequences.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

    fn trade(price: &str, timestamp: i64) -> Trade {
        Trade {
            exchange: CexId::Binance,
            trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            price: Decimal::from_str_exact(price).unwrap(),
            quantity: Decimal::ONE,
            timestamp: TimestampMilliseconds(timestamp),
        }
    }

    fn event(sequence: u64, timestamp: i64) -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(timestamp + 1),
            payload: String::new(),
        }
    }

    fn one_minute() -> CandleInterval {
        CandleInterval::from_label("1m").unwrap()
    }

    fn gap(detected_at: i64) -> GapMarker {
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 1,
            observed_sequence: 3,
            missing_from: 2,
            missing_to: 2,
            detected_at: TimestampMilliseconds(detected_at),
        }
    }

    #[test]
    fn configured_schema_version_is_used_for_new_and_restored_state() {
        let config = ProcessorConfig::new(vec![one_minute()], DurationMilliseconds::ZERO)
            .with_state_schema_version(9);

        let processor = CandleProcessor::with_config(config.clone());
        assert_eq!(processor.state().schema_version, 9);

        let restored = CandleProcessor::from_state(config.clone(), processor.into_state()).unwrap();
        assert_eq!(restored.state().schema_version, 9);
        assert!(CandleProcessor::from_state(config, AggregatorState::new()).is_err());
    }

    #[test]
    fn first_trade_opens_a_candle() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        let update = processor.apply_trade(&trade("100", 61_000));

        assert!(update.closed.is_empty());
        assert_eq!(update.open.len(), 1);
        assert_eq!(update.open[0].start_time(), TimestampMilliseconds(60_000));
        assert_eq!(update.open[0].market_candle.ohlcv.open, 100.0);
        assert!(!update.open[0].market_candle.is_candle_closed);
    }

    #[test]
    fn crossing_a_boundary_closes_the_previous_candle_from_event_time() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));
        let update = processor.apply_trade(&trade("101", 121_000));

        assert_eq!(update.closed.len(), 1);
        assert_eq!(
            update.closed[0].revision.close_watermark,
            TimestampMilliseconds(121_000)
        );
        assert_eq!(update.closed[0].revision.source_offset, 0);
        assert_eq!(update.closed[0].revision.state_epoch, 0);
        assert!(update.closed[0].candle.market_candle.is_candle_closed);
        assert_eq!(
            update.closed[0].candle.start_time(),
            TimestampMilliseconds(60_000)
        );
        assert_eq!(update.open.len(), 1);
        assert_eq!(update.open[0].start_time(), TimestampMilliseconds(120_000));
    }

    #[test]
    fn closed_candle_revisions_are_reproducible_from_state() {
        let mut first = CandleProcessor::new(vec![one_minute()]);
        first.state_mut().mark_processed(42);
        first.apply_trade(&trade("100", 61_000));
        let first_update = first.apply_trade(&trade("101", 121_000));

        let mut replay = CandleProcessor::new(vec![one_minute()]);
        replay.state_mut().mark_processed(42);
        replay.apply_trade(&trade("100", 61_000));
        let replay_update = replay.apply_trade(&trade("101", 121_000));

        assert_eq!(
            first_update.closed[0].revision,
            replay_update.closed[0].revision
        );
        assert_eq!(
            first_update.closed[0].message_id(),
            replay_update.closed[0].message_id()
        );
        assert_eq!(first_update.closed[0].revision.source_offset, 42);
    }

    #[test]
    fn events_are_deduplicated_by_exchange_stream_and_sequence() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        let first = processor.apply_event(&event(7, 61_000), &[trade("100", 61_000)]);
        let duplicate = processor.apply_event(&event(7, 61_000), &[trade("100", 61_000)]);
        let next = processor.apply_event(&event(8, 62_000), &[trade("101", 62_000)]);

        assert_eq!(first.open[0].market_candle.trade_count, 1);
        assert_eq!(duplicate.duplicates, 1);
        assert!(duplicate.open.is_empty());
        assert_eq!(next.duplicates, 0);
        assert_eq!(
            processor
                .state()
                .open_candle(&CandleKey::new(
                    CexId::Binance,
                    TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                    one_minute()
                ))
                .unwrap()
                .market_candle
                .trade_count,
            2
        );
    }

    #[test]
    fn replaying_the_same_events_produces_identical_state() {
        let events = [
            (event(1, 61_000), trade("100", 61_000)),
            (event(2, 62_000), trade("101", 62_000)),
            (event(3, 121_000), trade("102", 121_000)),
            (event(2, 62_000), trade("101", 62_000)),
        ];

        let mut first = CandleProcessor::new(vec![one_minute()]);
        let mut second = CandleProcessor::new(vec![one_minute()]);
        for (event, trade) in &events {
            first.apply_event(event, std::slice::from_ref(trade));
        }
        for (event, trade) in &events {
            second.apply_event(event, std::slice::from_ref(trade));
        }

        assert_eq!(first.state(), second.state());
        assert_eq!(
            first.seen_sequences(CexId::Binance, "btcusdt@trade"),
            vec![SequenceRange::new(1, 3)]
        );
    }

    #[test]
    fn trades_inside_the_allowed_lateness_update_the_older_window() {
        let mut processor = CandleProcessor::with_config(ProcessorConfig::new(
            vec![one_minute()],
            DurationMilliseconds(5_000),
        ));
        processor.apply_trade(&trade("100", 61_000));
        let update = processor.apply_trade(&trade("90", 119_000));

        assert!(update.closed.is_empty());
        let candle = processor
            .state()
            .open_candle(&CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                one_minute(),
            ))
            .unwrap();
        assert_eq!(candle.market_candle.ohlcv.high, 100.0);
        assert_eq!(candle.market_candle.ohlcv.low, 90.0);
        assert_eq!(candle.market_candle.trade_count, 2);
    }

    #[test]
    fn trades_after_allowed_lateness_correct_the_closed_candle() {
        let mut processor = CandleProcessor::with_config(ProcessorConfig::new(
            vec![one_minute()],
            DurationMilliseconds(5_000),
        ));
        processor.apply_trade(&trade("100", 61_000));
        processor.flush_closed_at_or_before(TimestampMilliseconds(125_000));
        let update = processor.apply_trade(&trade("90", 61_500));

        assert_eq!(update.late_trades, 1);
        assert!(update.open.is_empty());
        assert_eq!(update.closed.len(), 1);
        assert_eq!(update.closed[0].candle.market_candle.trade_count, 2);
        assert_eq!(update.closed[0].candle.market_candle.ohlcv.low, 90.0);
    }

    #[test]
    fn far_future_exchange_timestamps_cannot_advance_the_watermark_past_receipt() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        let mut far_future = event(1, 9_000_000_000_000);
        far_future.received_at = TimestampMilliseconds(62_000);

        processor.apply_event(&far_future, &[]);
        assert_eq!(
            processor.state().watermark,
            Some(TimestampMilliseconds(62_000))
        );
        assert_eq!(
            processor
                .state()
                .stream_watermark(CexId::Binance, "btcusdt@trade"),
            Some(TimestampMilliseconds(62_000))
        );

        let update = processor.apply_event(&event(2, 63_000), &[trade("100", 63_000)]);
        assert_eq!(update.late_trades, 0);
        assert_eq!(update.open[0].market_candle.trade_count, 1);
    }

    #[test]
    fn idle_flushes_do_not_push_event_time_stream_watermarks() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_event(&event(1, 61_000), &[trade("100", 61_000)]);

        let closed = processor.flush_closed_at_or_before(TimestampMilliseconds(125_000));
        assert_eq!(closed.len(), 1);
        assert_eq!(
            processor
                .state()
                .stream_watermark(CexId::Binance, "btcusdt@trade"),
            Some(TimestampMilliseconds(61_000))
        );

        let update = processor.apply_event(&event(2, 61_500), &[trade("105", 61_500)]);
        assert_eq!(update.late_trades, 1);
        assert_eq!(update.closed.len(), 1);
        assert_eq!(update.closed[0].candle.market_candle.trade_count, 2);
    }

    #[test]
    fn stream_watermarks_do_not_close_other_pairs_candles() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_event(&event(1, 61_000), &[trade("100", 61_000)]);

        let mut ethereum_event = event(2, 500_000);
        ethereum_event.stream = "ethusdt@trade".to_string();
        let mut ethereum_trade = trade("200", 500_000);
        ethereum_trade.trading_pair = TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD);
        processor.apply_event(&ethereum_event, &[ethereum_trade]);

        let update = processor.apply_event(&event(3, 62_000), &[trade("101", 62_000)]);

        assert_eq!(update.late_trades, 0);
        let key = CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            one_minute(),
        );
        let candle = processor.state().open_candle(&key).unwrap();
        assert_eq!(candle.start_time(), TimestampMilliseconds(60_000));
        assert_eq!(candle.market_candle.trade_count, 2);
        assert!(!candle.market_candle.is_candle_closed);
    }

    #[test]
    fn backfilled_trades_revise_closed_candles() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.state_mut().mark_processed(1);
        processor.apply_trade(&trade("100", 61_000));
        processor.state_mut().mark_processed(2);
        let original = processor.apply_trade(&trade("101", 121_000));
        processor.state_mut().mark_processed(3);
        let update = processor.apply_trade(&trade("105", 61_500));

        assert_eq!(update.late_trades, 1);
        assert!(update.open.is_empty());
        assert_eq!(update.closed.len(), 1);
        assert_eq!(
            update.closed[0].candle.start_time(),
            TimestampMilliseconds(60_000)
        );
        assert_eq!(update.closed[0].candle.market_candle.trade_count, 2);
        assert_eq!(update.closed[0].candle.market_candle.ohlcv.close, 105.0);
        assert_ne!(
            update.closed[0].message_id(),
            original.closed[0].message_id()
        );
        let key = CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            one_minute(),
        );
        let retained = processor
            .state()
            .closed_candle(&key, TimestampMilliseconds(60_000))
            .unwrap();
        assert_eq!(retained.market_candle.trade_count, 2);
        assert_eq!(retained.market_candle.ohlcv.close, 105.0);
    }

    #[test]
    fn backfilled_trades_create_candles_for_windows_without_history() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 121_000));
        let update = processor.apply_trade(&trade("90", 61_000));

        assert_eq!(update.late_trades, 1);
        assert!(update.open.is_empty());
        assert_eq!(update.closed.len(), 1);
        assert_eq!(
            update.closed[0].candle.start_time(),
            TimestampMilliseconds(60_000)
        );
        assert_eq!(update.closed[0].candle.market_candle.trade_count, 1);
    }

    #[test]
    fn quiet_event_time_does_not_close_candles_with_the_wall_clock() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));
        let update = processor.apply_event(&event(2, 62_000), &[]);

        assert!(update.closed.is_empty());
        assert_eq!(processor.state().open_candles_len(), 1);
    }

    #[test]
    fn a_gap_records_an_incomplete_window_and_closes_expired_candles() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));

        let update = processor.apply_gap(&gap(120_000));

        assert_eq!(update.closed.len(), 1);
        assert!(update.closed[0].candle.market_candle.is_candle_closed);
        assert!(update.closed[0].is_incomplete());
        assert_eq!(update.incomplete_windows.len(), 1);
        assert_eq!(
            update.incomplete_windows[0].window_start,
            TimestampMilliseconds(60_000)
        );
        assert_eq!(update.incomplete_windows[0].from_sequence, 2);
        assert_eq!(update.incomplete_windows[0].to_sequence, 2);
        // The closed window no longer needs to be tracked once the flag has
        // been carried onto the candle.
        assert!(processor.state().incomplete_windows.is_empty());
    }

    #[test]
    fn a_gap_marks_the_open_candle_and_republishes_it() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));

        let update = processor.apply_gap(&GapMarker {
            detected_at: TimestampMilliseconds(65_000),
            ..gap(65_000)
        });

        assert_eq!(update.open.len(), 1);
        assert!(update.open[0].is_incomplete());
        let candle = processor
            .state()
            .open_candle(&CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                one_minute(),
            ))
            .unwrap();
        assert!(candle.is_incomplete());
    }

    #[test]
    fn a_gap_marks_a_retained_closed_candle_incomplete() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));
        let closed = processor.apply_trade(&trade("101", 121_000));
        assert!(closed.closed[0].is_complete());

        let update = processor.apply_gap(&GapMarker {
            detected_at: TimestampMilliseconds(65_000),
            ..gap(65_000)
        });

        assert_eq!(update.closed.len(), 1);
        assert!(update.closed[0].is_incomplete());
        let key = CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            one_minute(),
        );
        let retained = processor
            .state()
            .closed_candle(&key, TimestampMilliseconds(60_000))
            .unwrap();
        assert!(retained.is_incomplete());
    }

    #[test]
    fn pruned_gap_bookkeeping_does_not_grow_without_bound() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));
        processor.apply_gap(&gap(120_000));

        assert!(processor.state().known_gaps.is_empty());
        assert!(processor.state().incomplete_windows.is_empty());
    }

    #[test]
    fn gaps_only_mark_windows_for_their_stream_pair() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));
        let mut ethereum_trade = trade("200", 62_000);
        ethereum_trade.trading_pair = TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD);
        processor.apply_trade(&ethereum_trade);

        let update = processor.apply_gap(&gap(120_000));

        assert_eq!(update.incomplete_windows.len(), 1);
        assert_eq!(
            update.incomplete_windows[0].key.trading_pair,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
        );
    }

    #[test]
    fn open_updates_are_coalesced_by_key_keeping_the_last_value() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        let update = processor.apply_event(
            &event(1, 62_000),
            &[trade("100", 61_000), trade("101", 61_500)],
        );

        assert_eq!(update.open.len(), 1);
        assert_eq!(update.open[0].market_candle.trade_count, 2);
        assert_eq!(update.open[0].market_candle.ohlcv.close, 101.0);
    }

    #[test]
    fn update_includes_every_configured_interval() {
        let mut processor = CandleProcessor::new(vec![
            CandleInterval::from_label("1m").unwrap(),
            CandleInterval::from_label("5m").unwrap(),
        ]);
        let update = processor.apply_trade(&trade("100", 61_000));

        assert_eq!(update.open.len(), 2);
        assert_eq!(update.open[0].key.interval.label(), "1m");
        assert_eq!(update.open[1].key.interval.label(), "5m");
    }

    #[test]
    fn late_trades_do_not_reopen_a_closed_candle() {
        let mut processor = CandleProcessor::new(vec![one_minute()]);
        processor.apply_trade(&trade("100", 61_000));
        processor.apply_trade(&trade("101", 121_000));
        let update = processor.apply_trade(&trade("90", 61_500));

        assert_eq!(update.late_trades, 1);
        assert!(update.open.is_empty());
        assert_eq!(update.closed.len(), 1);
        assert_eq!(processor.state().open_candles_len(), 1);
    }
}
