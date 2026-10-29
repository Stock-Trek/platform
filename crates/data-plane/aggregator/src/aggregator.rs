use crate::state::AggregatorState;
use platform_errors::BpResult;
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    decoders::trade::Trade,
    event::GapMarker,
};
use platform_metrics::descriptions::{DROP_REASON_OUT_OF_ORDER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_types::new_types::TimestampMilliseconds;

/// Candles updated by a single trade.
#[derive(Debug, Default, Clone)]
pub struct AggregationUpdate {
    /// Candles that need to be written back to the open-candle store.
    pub open: Vec<Candle>,
    /// Candles that just closed and need to be published.
    pub closed: Vec<ClosedCandle>,
    /// Trades ignored because they predate the currently open candle.
    pub late_trades: usize,
}

impl AggregationUpdate {
    pub fn is_empty(&self) -> bool {
        self.open.is_empty() && self.closed.is_empty() && self.late_trades == 0
    }
}

/// Maintains the currently open candle for every configured interval.
///
/// The aggregator assigns a placeholder revision of zero; the closed-candle
/// publisher assigns the authoritative revision when publishing.
#[derive(Debug)]
pub struct CandleAggregator {
    intervals: Vec<CandleInterval>,
    state: AggregatorState,
}

impl CandleAggregator {
    pub fn new(intervals: Vec<CandleInterval>) -> Self {
        Self {
            intervals,
            state: AggregatorState::new(),
        }
    }

    /// Restores an aggregator from a durable state snapshot.
    pub fn from_state(intervals: Vec<CandleInterval>, state: AggregatorState) -> BpResult<Self> {
        state.ensure_schema_compatible()?;
        Ok(Self { intervals, state })
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

    pub fn apply_trade(&mut self, trade: &Trade) -> AggregationUpdate {
        self.state.advance_watermark(trade.timestamp);
        let mut update = AggregationUpdate {
            open: Vec::with_capacity(self.intervals.len()),
            closed: Vec::new(),
            late_trades: 0,
        };
        let mut is_late = false;

        // Iterate by index instead of cloning the interval list on every trade.
        // `CandleInterval` is `Copy`, so reading the configured interval is cheap.
        for index in 0..self.intervals.len() {
            let interval = self.intervals[index];
            let key = CandleKey::new(trade.exchange, trade.trading_pair, interval);
            let candle_start = interval.start_for(trade.timestamp);
            let current_start = self.state.open_candles.get(&key).map(Candle::start_time);

            match current_start {
                Some(start) if start == candle_start => {
                    if let Some(candle) = self.state.open_candles.get_mut(&key) {
                        candle.apply_trade(trade);
                        update.open.push(candle.clone());
                    }
                }
                Some(start) if start < candle_start => {
                    if let Some(closed) = self.close_existing(&key) {
                        update.closed.push(closed);
                    }
                    let candle = Candle::open(trade, interval);
                    self.state.open_candles.insert(key, candle.clone());
                    update.open.push(candle);
                }
                Some(_) => {
                    // A late trade cannot change a closed candle. Keep the
                    // current open candle untouched.
                    is_late = true;
                }
                None => {
                    let candle = Candle::open(trade, interval);
                    self.state.open_candles.insert(key, candle.clone());
                    update.open.push(candle);
                }
            }
        }

        if is_late {
            update.late_trades += 1;
            MarketDataMetrics::record_dropped_event(STAGE_AGGREGATOR, DROP_REASON_OUT_OF_ORDER);
        }

        update
    }

    /// Applies a gap marker by closing every candle window that ended before
    /// the gap was observed.
    ///
    /// Gap markers prove that the ingester skipped a sequence range. Any candle
    /// whose window finished before the observed event is complete even though
    /// its trades may never arrive; the current window is left open.
    pub fn apply_gap(&mut self, gap: &GapMarker) -> Vec<ClosedCandle> {
        self.state.advance_watermark(gap.detected_at);
        self.state.record_gap(gap);
        self.flush_closed_at_or_before(gap.detected_at)
    }

    /// Closes every open candle whose window ends at or before `timestamp`.
    ///
    /// This is called periodically so quiet markets still produce closed
    /// candles even when no subsequent trade arrives.
    pub fn flush_closed_at_or_before(
        &mut self,
        timestamp: TimestampMilliseconds,
    ) -> Vec<ClosedCandle> {
        let mut keys: Vec<CandleKey> = self
            .state
            .open_candles
            .iter()
            .filter(|(_, candle)| candle.end_time() <= timestamp)
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort_by_key(CandleKey::subject_suffix);

        keys.iter()
            .filter_map(|key| self.close_existing(key))
            .collect()
    }

    fn close_existing(&mut self, key: &CandleKey) -> Option<ClosedCandle> {
        let mut candle = self.state.open_candles.remove(key)?;
        candle.close();
        Some(ClosedCandle {
            revision: 0,
            candle,
        })
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

    fn one_minute() -> CandleInterval {
        CandleInterval::from_label("1m").unwrap()
    }

    #[test]
    fn first_trade_opens_a_candle() {
        let mut aggregator = CandleAggregator::new(vec![one_minute()]);
        let update = aggregator.apply_trade(&trade("100", 61_000));

        assert!(update.closed.is_empty());
        assert_eq!(update.open.len(), 1);
        assert_eq!(update.open[0].start_time(), TimestampMilliseconds(60_000));
        assert_eq!(update.open[0].market_candle.ohlcv.open, 100.0);
        assert!(!update.open[0].market_candle.is_candle_closed);
    }

    #[test]
    fn crossing_a_boundary_closes_and_revises_the_previous_candle() {
        let mut aggregator = CandleAggregator::new(vec![one_minute()]);
        aggregator.apply_trade(&trade("100", 61_000));
        let update = aggregator.apply_trade(&trade("101", 121_000));

        assert_eq!(update.closed.len(), 1);
        assert_eq!(update.closed[0].revision, 0);
        assert!(update.closed[0].candle.market_candle.is_candle_closed);
        assert_eq!(
            update.closed[0].candle.start_time(),
            TimestampMilliseconds(60_000)
        );
        assert_eq!(update.open.len(), 1);
        assert_eq!(update.open[0].start_time(), TimestampMilliseconds(120_000));

        let next = aggregator.apply_trade(&trade("102", 181_000));
        assert_eq!(next.closed[0].revision, 0);
    }

    #[test]
    fn update_includes_every_configured_interval() {
        let mut aggregator = CandleAggregator::new(vec![
            CandleInterval::from_label("1m").unwrap(),
            CandleInterval::from_label("5m").unwrap(),
        ]);
        let update = aggregator.apply_trade(&trade("100", 61_000));

        assert_eq!(update.open.len(), 2);
        assert_eq!(update.open[0].key.interval.label(), "1m");
        assert_eq!(update.open[1].key.interval.label(), "5m");
    }

    #[test]
    fn periodic_flush_closes_expired_candles() {
        let mut aggregator = CandleAggregator::new(vec![one_minute()]);
        aggregator.apply_trade(&trade("100", 61_000));

        assert!(
            aggregator
                .flush_closed_at_or_before(TimestampMilliseconds(119_999))
                .is_empty()
        );
        let closed = aggregator.flush_closed_at_or_before(TimestampMilliseconds(120_000));
        assert_eq!(closed.len(), 1);
        assert!(closed[0].candle.market_candle.is_candle_closed);
        assert_eq!(closed[0].revision, 0);
    }

    #[test]
    fn a_gap_closes_windows_that_ended_before_it_was_observed() {
        let mut aggregator = CandleAggregator::new(vec![one_minute()]);
        aggregator.apply_trade(&trade("100", 61_000));
        let gap = GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 1,
            observed_sequence: 3,
            missing_from: 2,
            missing_to: 2,
            detected_at: TimestampMilliseconds(120_000),
        };

        let closed = aggregator.apply_gap(&gap);

        assert_eq!(closed.len(), 1);
        assert!(closed[0].candle.market_candle.is_candle_closed);
    }

    #[test]
    fn late_trades_do_not_reopen_a_candle() {
        let mut aggregator = CandleAggregator::new(vec![one_minute()]);
        aggregator.apply_trade(&trade("100", 121_000));
        let update = aggregator.apply_trade(&trade("90", 61_000));

        assert!(!update.is_empty());
        assert!(update.open.is_empty());
        assert!(update.closed.is_empty());
        assert_eq!(update.late_trades, 1);
    }

    #[test]
    fn a_late_trade_is_counted_once_across_all_intervals() {
        let mut aggregator = CandleAggregator::new(vec![
            CandleInterval::from_label("1m").unwrap(),
            CandleInterval::from_label("5m").unwrap(),
        ]);
        aggregator.apply_trade(&trade("100", 301_000));
        let update = aggregator.apply_trade(&trade("90", 61_000));

        assert_eq!(update.late_trades, 1);
    }
}
