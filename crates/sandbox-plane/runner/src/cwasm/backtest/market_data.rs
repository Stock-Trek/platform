use crate::cwasm::backtest::{BacktestError, PlatformResult};
use platform_runner::{
    dto::{MarketCandleDto, MarketOhlcvDto, StrategyContextDto},
    string::JsonString,
};
use std::collections::BTreeMap;
use stock_trek::{
    markets::{AlignedWindow, TimestampMillis},
    types::{AssetId, CexId},
};

pub type MarketData = BTreeMap<CexId, BTreeMap<(AssetId, AssetId), BTreeMap<u32, Series>>>;

#[derive(Clone)]
pub struct Series {
    pub candles: Vec<MarketCandleDto>,
    pub ends: Vec<u64>,
}

impl Series {
    pub fn load(
        market_data: &StrategyContextDto,
        pairs: &[(AssetId, AssetId)],
        window_numbers: &[u32],
    ) -> PlatformResult<MarketData> {
        let mut data: MarketData = BTreeMap::new();
        for (cex_name, asset_markets) in &market_data.exchange_asset_markets_dto {
            let cex: CexId = JsonString::deserialize(cex_name).map_err(|error| {
                BacktestError::invalid_market_data(format!("cex is invalid: {error}"))
            })?;
            for market_by_base in &asset_markets.markets_by_base_dto {
                let base: AssetId = market_by_base
                    .base_asset_id_dto
                    .as_ref()
                    .ok_or_else(|| BacktestError::invalid_market_data("base asset is missing"))
                    .and_then(|dto| {
                        JsonString::deserialize(&dto.asset_id).map_err(|error| {
                            BacktestError::invalid_market_data(format!(
                                "base asset is invalid: {error}"
                            ))
                        })
                    })?;
                for market_by_quote in &market_by_base.markets_by_quote_dto {
                    let quote: AssetId = market_by_quote
                        .quote_asset_id_dto
                        .as_ref()
                        .ok_or_else(|| BacktestError::invalid_market_data("quote asset is missing"))
                        .and_then(|dto| {
                            JsonString::deserialize(&dto.asset_id).map_err(|error| {
                                BacktestError::invalid_market_data(format!(
                                    "quote asset is invalid: {error}"
                                ))
                            })
                        })?;
                    if !pairs.contains(&(base, quote)) {
                        continue;
                    }
                    let Some(market) = market_by_quote.market_dto.as_ref() else {
                        return Err(BacktestError::invalid_market_data("market is missing").into());
                    };
                    for (number, candles_dto) in &market.aligned_dto {
                        if !window_numbers.contains(number) {
                            continue;
                        }
                        let mut candles = candles_dto.candles_dto.clone();
                        candles.sort_by_key(|candle| candle.start_time_millis_inc);
                        Self::validate(&candles)?;
                        let window =
                            AlignedWindow::from_repr(u8::try_from(*number).map_err(|_| {
                                BacktestError::invalid_market_data("window is invalid")
                            })?)
                            .ok_or_else(|| {
                                BacktestError::invalid_market_data("window is invalid")
                            })?;
                        let ends = candles
                            .iter()
                            .map(|candle| Self::candle_end(candle, window))
                            .collect::<PlatformResult<Vec<u64>>>()?;
                        data.entry(cex)
                            .or_default()
                            .entry((base, quote))
                            .or_default()
                            .insert(*number, Series { candles, ends });
                    }
                }
            }
        }
        for cex in data.keys().copied().collect::<Vec<_>>() {
            for pair in pairs {
                if !data[&cex].contains_key(pair) {
                    return Err(BacktestError::invalid_argument(format!(
                        "market data has no {pair:?} series for {cex}"
                    ))
                    .into());
                }
            }
            for ((base, quote), series) in &data[&cex] {
                for number in window_numbers {
                    if !series.contains_key(number) {
                        return Err(BacktestError::invalid_argument(format!(
                            "market data has no window {number} series for {cex} {base}/{quote}"
                        ))
                        .into());
                    }
                }
            }
        }
        Ok(data)
    }

    fn candle_end(candle: &MarketCandleDto, window: AlignedWindow) -> PlatformResult<u64> {
        Ok(window
            .bounds_for(TimestampMillis::new(candle.start_time_millis_inc))
            .map(|bounds| bounds.upper().as_millis())
            .map_err(|error| {
                BacktestError::invalid_market_data(format!("candle bounds are invalid: {error}"))
            })?)
    }

    fn validate(candles: &[MarketCandleDto]) -> PlatformResult<()> {
        for candle in candles {
            let ohlcv: &MarketOhlcvDto = candle
                .ohlcv_dto
                .as_ref()
                .ok_or_else(|| BacktestError::invalid_market_data("candle ohlcv is missing"))?;
            for (name, value) in [
                ("open", &ohlcv.open_dto),
                ("high", &ohlcv.high_dto),
                ("low", &ohlcv.low_dto),
                ("close", &ohlcv.close_dto),
                ("volume", &ohlcv.volume_dto),
                ("quote volume", &ohlcv.quote_volume_dto),
            ] {
                if value.is_none() {
                    return Err(BacktestError::invalid_market_data(format!(
                        "candle {name} is missing"
                    ))
                    .into());
                }
            }
        }
        Ok(())
    }
}
