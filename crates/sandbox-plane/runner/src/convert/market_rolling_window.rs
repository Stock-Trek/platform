use crate::generated::dto::MarketCandleDto;
use std::collections::HashMap;
use stock_trek::{
    errors::{StockTrekError, ValueError},
    markets::{MarketRollingWindow, RollingWindow},
};

pub struct MarketCandleMap(pub HashMap<u32, MarketCandleDto>);

impl TryFrom<MarketCandleMap> for MarketRollingWindow {
    type Error = StockTrekError;

    fn try_from(value: MarketCandleMap) -> Result<Self, Self::Error> {
        let mut candles = HashMap::new();
        for (window_u32, candle_dto) in value.0 {
            let window = u8::try_from(window_u32)
                .ok()
                .and_then(RollingWindow::from_repr)
                .ok_or(StockTrekError::Value(ValueError::IncorrectType {
                    expected: "RollingWindow".to_string(),
                    found: window_u32.to_string(),
                }))?;
            let candle = candle_dto.try_into()?;
            candles.insert(window, candle);
        }
        Ok(MarketRollingWindow::new(candles))
    }
}
