use crate::{
    convert::{required::IsRequired, string::JsonString},
    generated::dto::{AssetMarketsDto, MarketByBaseDto, MarketByQuoteDto, StrategyContextDto},
};
use std::collections::HashMap;
use stock_trek::{
    errors::StockTrekError,
    signals::{CexMarketDataByBaseContext, CexMarketDataByQuoteContext, SignalContext},
    types::CexId,
};

impl TryFrom<StrategyContextDto> for SignalContext {
    type Error = StockTrekError;

    fn try_from(value: StrategyContextDto) -> Result<Self, Self::Error> {
        let StrategyContextDto {
            exchange_asset_markets_dto,
        } = value;
        let mut market_data = HashMap::new();
        for (cex_name, asset_markets_dto) in exchange_asset_markets_dto {
            let cex_id: CexId = JsonString::deserialize(&cex_name)?;
            market_data.insert(cex_id, asset_markets_dto.try_into()?);
        }
        SignalContext::new(market_data)
    }
}

impl TryFrom<AssetMarketsDto> for CexMarketDataByBaseContext {
    type Error = StockTrekError;

    fn try_from(value: AssetMarketsDto) -> Result<Self, Self::Error> {
        let AssetMarketsDto {
            markets_by_base_dto,
        } = value;
        let mut markets_by_base = HashMap::new();
        for market_by_base in markets_by_base_dto {
            let MarketByBaseDto {
                base_asset_id_dto,
                markets_by_quote_dto,
            } = market_by_base;
            let base_asset_id = base_asset_id_dto
                .required("base_asset_id_dto")?
                .try_into()?;
            let mut markets_by_quote = HashMap::new();
            for market_by_quote_dto in markets_by_quote_dto {
                let MarketByQuoteDto {
                    quote_asset_id_dto,
                    market_dto,
                } = market_by_quote_dto;
                let quote_asset_id = quote_asset_id_dto
                    .required("quote_asset_id_dto")?
                    .try_into()?;
                let market = market_dto.required("market_dto")?.try_into()?;
                markets_by_quote.insert(quote_asset_id, market);
            }
            let markets_by_quote = CexMarketDataByQuoteContext::new(markets_by_quote)?;
            markets_by_base.insert(base_asset_id, markets_by_quote);
        }
        CexMarketDataByBaseContext::new(markets_by_base)
    }
}
