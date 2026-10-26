use crate::{
    convert::{
        market_aligned_window::MarketCandlesMap, market_rolling_window::MarketCandleMap,
        market_tick::MarketTickVec, required::IsRequired,
    },
    generated::dto::MarketDto,
};
use stock_trek::{errors::StockTrekError, markets::Market};

impl TryFrom<MarketDto> for Market {
    type Error = StockTrekError;

    fn try_from(value: MarketDto) -> Result<Self, Self::Error> {
        let MarketDto {
            aligned_dto,
            base_increment_dto,
            minimum_notional_dto,
            order_book_dto,
            quote_increment_dto,
            rolling_dto,
            ticks_dto,
        } = value;
        let base_increment = base_increment_dto
            .required("base_increment_dto")?
            .try_into()?;
        let quote_increment = quote_increment_dto
            .required("quote_increment_dto")?
            .try_into()?;
        let minimum_notional = minimum_notional_dto
            .required("minimum_notional_dto")?
            .try_into()?;
        let ticks = MarketTickVec(ticks_dto.required("ticks_dto")?.ticks_dto).try_into()?;
        let order_book = order_book_dto.required("order_book_dto")?.try_into()?;
        let rolling = MarketCandleMap(rolling_dto).try_into()?;
        let aligned = MarketCandlesMap(aligned_dto).try_into()?;
        Ok(Market {
            base_increment,
            quote_increment,
            minimum_notional,
            ticks,
            rolling,
            aligned,
            order_book,
        })
    }
}
