use crate::{convert::required::IsRequired, generated::dto::MarketCandleDto};
use stock_trek::{
    errors::{StockTrekError, ValueError},
    markets::{MarketCandle, RollingWindow, TimestampMillis},
};

impl TryFrom<MarketCandleDto> for MarketCandle {
    type Error = StockTrekError;

    fn try_from(value: MarketCandleDto) -> Result<Self, Self::Error> {
        let MarketCandleDto {
            window,
            start_time_millis_inc,
            is_candle_closed,
            ohlcv_dto,
            trade_count,
        } = value;
        let window = u8::try_from(window)
            .ok()
            .and_then(RollingWindow::from_repr)
            .ok_or(StockTrekError::Value(ValueError::IncorrectType {
                expected: "RollingWindow".to_string(),
                found: window.to_string(),
            }))?;
        MarketCandle::new(
            window,
            TimestampMillis::new(start_time_millis_inc),
            is_candle_closed,
            ohlcv_dto.required("ohlcv_dto")?.try_into()?,
            trade_count,
        )
    }
}
