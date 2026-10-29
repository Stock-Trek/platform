//! Deterministic candle aggregation.
//!
//! [`CandleProcessor`] is a pure fold over keyed state: it never reads the wall
//! clock and performs no I/O. The worker persists the updated state and emits
//! the returned outputs, which makes replaying an event log reproducible.

use crate::state::{AggregatorState, DeduplicationKey, IncompleteWindow, SequenceRange, stream_id};
use platform_errors::BpResult;
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    decoders::trade::Trade,
    event::{GapMarker, MarketEvent},
};
use platform_metrics::descriptions::{
    DROP_REASON_DUPLICATE, DROP_REASON_OUT_OF_ORDER, STAGE_AGGREGATOR,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};

/// Interval and lateness settings shared by live processing and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorConfig {
    pub intervals: Vec<CandleInterval>,
    pub allowed_lateness: DurationMilliseconds,
}

impl ProcessorConfig {
    pub fn new(intervals: Vec<CandleInterval>, allowed_lateness: DurationMilliseconds) -> Self {
        Self {
            intervals,
            allowed_lateness,
        }
    }

    pub fn with_intervals(intervals: Vec<CandleInterval>) -> Self {
        Self::new(intervals, DurationMilliseconds::ZERO)
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
    /// Trades that arrived after their candle's allowed-lateness window closed.
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
        Self {
            config,
            state: AggregatorState::new(),
        }
    }

    pub fn from_state(config: ProcessorConfig, state: AggregatorState) -> BpResult<Self> {
        state.ensure_schema_compatible()?;
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
    /// watermark is advanced from the event's exchange timestamp before trades
    /// are folded in, and closing uses that event-time watermark plus the
    /// configured allowed lateness.
    pub fn apply_event(&mut self, event: &MarketEvent, trades: &[Trade]) -> ProcessorUpdate {
        let mut update = ProcessorUpdate::default();
        let timestamp = event.exchange_timestamp.unwrap_or(event.received_at);

        if let Some(sequence) = event.sequence {
            let key = DeduplicationKey::new(event.exchange, &event.stream, sequence);
            if !self.state.observe_sequence(&key) {
                update.duplicates += 1;
                MarketDataMetrics::record_dropped_event(STAGE_AGGREGATOR, DROP_REASON_DUPLICATE);
                return update;
            }
        }

        self.state
            .advance_stream_watermark(event.exchange, &event.stream, timestamp);
        self.state.advance_watermark(timestamp);

        for trade in trades {
            self.apply_trade_inner(trade, &mut update);
        }
        self.close_expired(&mut update);
        update
    }

    /// Applies one trade directly. This is used by recovery and unit tests where
    /// there is no enclosing market event.
    pub fn apply_trade(&mut self, trade: &Trade) -> ProcessorUpdate {
        let mut update = ProcessorUpdate::default();
        self.state.advance_watermark(trade.timestamp);
        self.apply_trade_inner(trade, &mut update);
        self.close_expired(&mut update);
        update
    }

    /// Records a gap as an incompleteness signal for the affected stream and
    /// candle windows, advances the event-time watermark, and closes windows
    /// whose allowed-lateness window has elapsed.
    pub fn apply_gap(&mut self, gap: &GapMarker) -> ProcessorUpdate {
        let mut update = ProcessorUpdate::default();
        let previous_stream_watermark = self.state.stream_watermark(gap.exchange, &gap.stream);

        self.state
            .advance_stream_watermark(gap.exchange, &gap.stream, gap.detected_at);
        self.state.advance_watermark(gap.detected_at);
        self.state.record_gap(gap);
        self.mark_affected_windows(gap, previous_stream_watermark, &mut update);
        self.close_expired(&mut update);
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
        self.close_expired(&mut update);
        update.closed
    }

    fn apply_trade_inner(&mut self, trade: &Trade, update: &mut ProcessorUpdate) {
        for index in 0..self.config.intervals.len() {
            let interval = self.config.intervals[index];
            let key = CandleKey::new(trade.exchange, trade.trading_pair, interval);
            let candle_start = interval.start_for(trade.timestamp);
            let candle_end = interval.end_for(trade.timestamp);

            if self.is_closed_window(candle_end) {
                update.late_trades += 1;
                MarketDataMetrics::record_dropped_event(STAGE_AGGREGATOR, DROP_REASON_OUT_OF_ORDER);
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

    fn is_closed_window(&self, window_end: TimestampMilliseconds) -> bool {
        let Some(watermark) = self.state.watermark else {
            return false;
        };
        window_end + self.config.allowed_lateness <= watermark
    }

    fn close_expired(&mut self, update: &mut ProcessorUpdate) {
        let Some(watermark) = self.state.watermark else {
            return;
        };
        let cutoff = watermark - self.config.allowed_lateness;
        let source_offset = self.state.last_processed_offset().unwrap_or_default();
        let state_epoch = self.state.state_epoch;
        let mut expired = Vec::new();
        for (key, candles) in &self.state.open_candles {
            for candle in candles {
                if candle.end_time() <= cutoff {
                    expired.push((key.clone(), candle.start_time().0));
                }
            }
        }
        expired.sort_by(|(left_key, left_start), (right_key, right_start)| {
            left_key
                .subject_suffix()
                .cmp(&right_key.subject_suffix())
                .then_with(|| left_start.cmp(right_start))
        });

        for (key, start) in expired {
            let Some(candles) = self.state.open_candles.get_mut(&key) else {
                continue;
            };
            let Ok(position) = candles.binary_search_by_key(&start, |candle| candle.start_time().0)
            else {
                continue;
            };
            let mut candle = candles.remove(position);
            candle.close();
            update.closed.push(ClosedCandle::new(
                candle,
                watermark,
                source_offset,
                state_epoch,
            ));
            if candles.is_empty() {
                self.state.open_candles.remove(&key);
            }
        }
    }

    fn mark_affected_windows(
        &mut self,
        gap: &GapMarker,
        previous_stream_watermark: Option<TimestampMilliseconds>,
        update: &mut ProcessorUpdate,
    ) {
        let affected = self
            .state
            .open_candles
            .iter()
            .flat_map(|(key, candles)| {
                candles
                    .iter()
                    .filter(|_| key.exchange == gap.exchange)
                    .filter(|candle| match previous_stream_watermark {
                        Some(previous) => {
                            candle.end_time() + self.config.allowed_lateness >= previous
                                && candle.start_time() <= gap.detected_at
                        }
                        None => candle.start_time() <= gap.detected_at,
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
            .collect::<Vec<_>>();

        for window in affected {
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
    fn trades_after_allowed_lateness_are_dropped() {
        let mut processor = CandleProcessor::with_config(ProcessorConfig::new(
            vec![one_minute()],
            DurationMilliseconds(5_000),
        ));
        processor.apply_trade(&trade("100", 61_000));
        processor.flush_closed_at_or_before(TimestampMilliseconds(125_000));
        let update = processor.apply_trade(&trade("90", 61_500));

        assert_eq!(update.late_trades, 1);
        assert!(update.open.is_empty());
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
        assert_eq!(update.incomplete_windows.len(), 1);
        assert_eq!(
            update.incomplete_windows[0].window_start,
            TimestampMilliseconds(60_000)
        );
        assert_eq!(update.incomplete_windows[0].from_sequence, 2);
        assert_eq!(update.incomplete_windows[0].to_sequence, 2);
        assert!(processor.state().incomplete_windows.len() == 1);
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
        processor.apply_trade(&trade("100", 121_000));
        let update = processor.apply_trade(&trade("90", 61_000));

        assert_eq!(update.late_trades, 1);
        assert!(update.open.is_empty());
        assert!(update.closed.is_empty());
    }
}
