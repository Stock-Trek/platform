use crate::{convert::required::IsRequired, generated::dto::MarketQuoteDto};
use stock_trek::{errors::StockTrekError, markets::MarketQuote};

impl TryFrom<MarketQuoteDto> for MarketQuote {
    type Error = StockTrekError;

    fn try_from(value: MarketQuoteDto) -> Result<Self, Self::Error> {
        let MarketQuoteDto {
            price_dto,
            quantity_dto,
        } = value;
        let price = price_dto.required("price_dto")?.try_into()?;
        let quantity = quantity_dto.required("quantity_dto")?.try_into()?;
        Ok(MarketQuote { price, quantity })
    }
}
