use platform_exit_codes::ExitCode;
use platform_runner::convert::decimal::ToDecimal;
use platform_runner::generated::dto::{
    DecimalDto, MarketCandleDto, MarketCandlesDto, StrategyContextDto,
};
use rand::{Rng, rngs::StdRng};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use stock_trek::markets::{AlignedWindow, TimestampMillis};

use crate::cwasm::backtest::{PriceMap, market_data::Series, portfolio::Portfolio, spec::Spec};

impl Spec {
    pub(super) fn step_context(
        &self,
        step: usize,
        rng: &mut StdRng,
    ) -> Result<(StrategyContextDto, PriceMap, u64), i32> {
        let mut context = self.strategy_context.clone();
        let mut prices: PriceMap = BTreeMap::new();
        let mut timestamp = u64::MAX;
        for slot in &self.market_slots {
            let driving = &slot.series[&self.driving_window_number];
            let candle = &driving.candles[step];
            let now = candle.start_time_millis_inc + self.open_candle_progress_millis;
            timestamp = timestamp.min(now);
            let ohlcv = candle
                .ohlcv_dto
                .as_ref()
                .ok_or(ExitCode::ServerRunnerMarketDataNotDecoded.code())?;
            let open_price = f64::try_from(
                ohlcv
                    .open_dto
                    .ok_or(ExitCode::ServerRunnerMarketDataNotDecoded.code())?,
            )
            .map_err(|_| ExitCode::ServerRunnerMarketDataNotDecoded.code())?;
            prices.insert((slot.cex, slot.pair.0, slot.pair.1), open_price);
            let Some(asset_markets) = context.exchange_asset_markets_dto.get_mut(&slot.cex_key)
            else {
                return Err(ExitCode::ServerEntryInvalidArgument.code());
            };
            let Some(market_by_base) = asset_markets.markets_by_base_dto.get_mut(slot.base_index)
            else {
                return Err(ExitCode::ServerEntryInvalidArgument.code());
            };
            let Some(market_by_quote) = market_by_base
                .markets_by_quote_dto
                .get_mut(slot.quote_index)
            else {
                return Err(ExitCode::ServerEntryInvalidArgument.code());
            };
            let Some(market) = market_by_quote.market_dto.as_mut() else {
                return Err(ExitCode::ServerEntryInvalidArgument.code());
            };
            let mut aligned = std::mem::take(&mut market.aligned_dto);
            for (number, window) in &self.windows {
                let series = slot
                    .series
                    .get(number)
                    .ok_or(ExitCode::ServerEntryInvalidArgument.code())?;
                let mut candles = series.window_candles(*window, now, self, rng)?;
                for candle in &mut candles.candles_dto {
                    candle.window = *number;
                }
                aligned.insert(*number, candles);
            }
            market.aligned_dto = aligned;
        }
        Ok((context, prices, timestamp))
    }
}

impl Series {
    fn window_candles(
        &self,
        window: AlignedWindow,
        now: u64,
        spec: &Spec,
        rng: &mut StdRng,
    ) -> Result<MarketCandlesDto, i32> {
        let closed_count = self.ends.partition_point(|&end| end <= now);
        let start = closed_count.saturating_sub(spec.historical_candle_count);
        let mut candles = self.candles[start..closed_count].to_vec();
        if let Some(open_start) = self.open_candle_start(window, now)?
            && let Some(candle) = self.open_candle(open_start, now, window)?
        {
            candles.push(candle);
        }
        if spec.simulation.candle_loss_chance > 0.0 {
            candles.retain(|_| rng.gen_range(0.0..1.0) >= spec.simulation.candle_loss_chance);
        }
        Ok(MarketCandlesDto {
            candles_dto: candles,
        })
    }

    fn open_candle_start(&self, window: AlignedWindow, now: u64) -> Result<Option<u64>, i32> {
        let bounds = window
            .bounds_for(TimestampMillis::new(now))
            .map_err(|error| {
                eprintln!("cwasm-invoker: could not compute window bounds: {error}");
                ExitCode::ServerRunnerMarketDataNotDecoded.code()
            })?;
        let lower = bounds.lower().as_millis();
        Ok(self
            .candles
            .binary_search_by_key(&lower, |candle| candle.start_time_millis_inc)
            .ok()
            .map(|_| lower))
    }

    fn open_candle(
        &self,
        start: u64,
        now: u64,
        window: AlignedWindow,
    ) -> Result<Option<MarketCandleDto>, i32> {
        let index = self
            .candles
            .binary_search_by_key(&start, |candle| candle.start_time_millis_inc)
            .map_err(|_| ExitCode::ServerRunnerMarketDataNotDecoded.code())?;
        let mut candle = self.candles[index];
        candle.is_candle_closed = false;
        let window_millis = window.millis().upper_bound();
        let proportion = (now - start) as f64 / window_millis as f64;
        if let Some(ohlcv) = candle.ohlcv_dto.as_mut() {
            if let Some(volume) = ohlcv
                .volume_dto
                .take()
                .and_then(|value| Self::scaled_decimal(value, proportion))
            {
                ohlcv.volume_dto = Some(volume);
            }
            if let Some(quote_volume) = ohlcv
                .quote_volume_dto
                .take()
                .and_then(|value| Self::scaled_decimal(value, proportion))
            {
                ohlcv.quote_volume_dto = Some(quote_volume);
            }
        }
        candle.trade_count = (candle.trade_count as f64 * proportion) as u64;
        Ok(Some(candle))
    }

    fn scaled_decimal(value: DecimalDto, proportion: f64) -> Option<DecimalDto> {
        let decimal = value.to_decimal().ok()?;
        let factor = Decimal::from_f64_retain(proportion)?;
        let scaled = decimal.checked_mul(factor)?;
        Some(Portfolio::decimal_dto(scaled))
    }
}
