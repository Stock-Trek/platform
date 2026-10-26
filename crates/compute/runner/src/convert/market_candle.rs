use crate::{convert::required::IsRequired, generated::dto::MarketCandleDto};
use stock_trek::{errors::StockTrekError, markets::MarketCandle};

impl TryFrom<MarketCandleDto> for MarketCandle {
    type Error = StockTrekError;

    fn try_from(value: MarketCandleDto) -> Result<Self, Self::Error> {
        let MarketCandleDto {
            start_time_millis_inc,
            end_time_millis_exc,
            duration_millis,
            is_candle_closed,
            ohlcv_dto,
            trade_count,
        } = value;
        Ok(MarketCandle::new(
            start_time_millis_inc,
            end_time_millis_exc,
            duration_millis,
            is_candle_closed,
            ohlcv_dto.required("ohlcv_dto")?.try_into()?,
            trade_count,
        ))
    }
}
