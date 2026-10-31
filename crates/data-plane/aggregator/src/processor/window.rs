//! Window expiry and gap handling.
//!
//! Event-time watermarks close candle windows once their allowed lateness has
//! elapsed. Source gaps mark the affected open or retained closed windows as
//! incomplete and publish those revisions.

use super::{CandleProcessor, ProcessorUpdate};
use crate::state::IncompleteWindow;
use platform_market_data::{candle::ClosedCandle, event::GapMarker};
use platform_types::new_types::TimestampMilliseconds;
use std::collections::HashSet;
use stock_trek_types::cex::trading_pair::TradingPair;
use tracing::warn;

impl CandleProcessor {
    pub(super) fn close_expired(&mut self, update: &mut ProcessorUpdate) {
        self.close_expired_at(None, update);
    }

    pub(super) fn close_expired_at(
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
            let evicted = self
                .state
                .retain_closed_candle(candle.clone(), self.config.max_closed_candles_per_key);
            update.evicted_closed_candles += evicted.len();
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

    pub(super) fn mark_affected_windows(
        &mut self,
        gap: &GapMarker,
        trading_pair: Option<TradingPair>,
        previous_stream_watermark: Option<TimestampMilliseconds>,
        update: &mut ProcessorUpdate,
    ) {
        let mut seen = HashSet::new();
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
        affected.retain(|window| {
            let watermark = self
                .watermark_for_pair(window.key.exchange, window.key.trading_pair)
                .or_else(|| self.global_watermark_fallback());
            if self.is_outside_correction_contract(window.window_end, watermark) {
                warn!(
                    exchange = %window.key.exchange,
                    base = %window.key.trading_pair.base,
                    quote = %window.key.trading_pair.quote,
                    interval = %window.key.interval,
                    window_start_ms = window.window_start.0,
                    window_end_ms = window.window_end.0,
                    watermark_ms = watermark.map(|watermark| watermark.0),
                    correction_cutoff_ms = self.config.correction_cutoff.0,
                    "skipped out-of-contract gap-affected candle window"
                );
                update.out_of_contract_gap_windows += 1;
                false
            } else {
                true
            }
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
}
