use bimap::BiMap;
use platform_exchange_types::ticker::Ticker;
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

#[derive(Debug, Clone)]
pub struct TickerMapping {
    pub map: BiMap<AssetId, Ticker>,
    pub symbol_separator: Option<u8>,
}

impl TickerMapping {
    pub fn asset_id(&self, ticker: &Ticker) -> Option<&AssetId> {
        self.map.get_by_right(ticker)
    }
    pub fn ticker(&self, asset_id: &AssetId) -> Option<&Ticker> {
        self.map.get_by_left(asset_id)
    }
    pub fn translate_ticker<'other>(
        &self,
        ticker: &Ticker,
        mapping: &'other TickerMapping,
    ) -> Option<&'other Ticker> {
        self.asset_id(ticker)
            .and_then(|asset_id| mapping.ticker(asset_id))
    }
    pub fn symbol(&self, trading_pair: &TradingPair) -> Option<String> {
        let base = self.ticker(&trading_pair.base)?;
        let quote = self.ticker(&trading_pair.quote)?;
        Some(match self.symbol_separator {
            Some(sep) => format!("{}{}{}", base.0, sep, quote.0),
            None => format!("{}{}", base.0, quote.0),
        })
    }
}
