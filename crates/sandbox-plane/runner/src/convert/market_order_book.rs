use crate::generated::dto::MarketOrderBookDto;
use stock_trek::{
    errors::StockTrekError,
    markets::{MarketOrderBook, MarketQuote},
};

impl TryFrom<MarketOrderBookDto> for MarketOrderBook {
    type Error = StockTrekError;

    fn try_from(value: MarketOrderBookDto) -> Result<Self, Self::Error> {
        let MarketOrderBookDto { bids_dto, asks_dto } = value;
        let bids = bids_dto
            .into_iter()
            .map(MarketQuote::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let asks = asks_dto
            .into_iter()
            .map(MarketQuote::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MarketOrderBook::new(bids, asks))
    }
}
