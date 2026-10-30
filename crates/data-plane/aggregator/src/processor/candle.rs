//! Folding trades into open and closed candles.
//!
//! A trade either updates an existing open window, revises a retained closed
//! candle, or creates a closed correction for a window that has already
//! expired. Event-time watermarks decide which case applies.

use super::{CandleProcessor, ProcessorUpdate};
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    decoders::trade::{Trade, TradeDecoderFactory},
};
use platform_metrics::descriptions::{DROP_REASON_INVALID_TRADE, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};

impl CandleProcessor {
    pub(super) fn apply_trade_inner(
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
                    let Some(candle) = candles.get_mut(position) else {
                        return;
                    };
                    if candle.apply_trade(trade).is_err() {
                        Self::record_invalid_trade();
                        return;
                    }
                    position
                }
                Err(position) => {
                    let Ok(candle) = Candle::open(trade, interval) else {
                        Self::record_invalid_trade();
                        return;
                    };
                    candles.insert(position, candle);
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
        if candle.apply_trade(trade).is_err() {
            Self::record_invalid_trade();
            return;
        }
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
        let Ok(mut candle) = Candle::open(trade, interval) else {
            Self::record_invalid_trade();
            return;
        };
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

    fn record_invalid_trade() {
        MarketDataMetrics::record_dropped_event(STAGE_AGGREGATOR, DROP_REASON_INVALID_TRADE);
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

    pub(super) fn global_watermark_fallback(&self) -> Option<TimestampMilliseconds> {
        if self.state.streams.is_empty() {
            self.state.watermark
        } else {
            None
        }
    }

    pub(super) fn watermark_for_pair(
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
}
