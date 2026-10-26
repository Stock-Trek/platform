use crate::{convert::required::IsRequired, generated::dto::MarketTickDto};
use stock_trek::{
    errors::StockTrekError,
    markets::{MarketTick, MarketTicks},
};

impl TryFrom<MarketTickDto> for MarketTick {
    type Error = StockTrekError;

    fn try_from(value: MarketTickDto) -> Result<Self, Self::Error> {
        let MarketTickDto {
            timestamp_millis,
            bid_dto,
            ask_dto,
            last_dto,
        } = value;
        let bid = bid_dto.required("bid_dto")?.try_into()?;
        let ask = ask_dto.required("ask_dto")?.try_into()?;
        let last = last_dto.required("last_dto")?.try_into()?;
        Ok(MarketTick {
            timestamp_millis,
            bid,
            ask,
            last,
        })
    }
}

pub struct MarketTickVec(pub Vec<MarketTickDto>);

impl TryFrom<MarketTickVec> for MarketTicks {
    type Error = StockTrekError;

    fn try_from(value: MarketTickVec) -> Result<Self, Self::Error> {
        Ok(MarketTicks::new(
            value
                .0
                .into_iter()
                .map(MarketTick::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
}
