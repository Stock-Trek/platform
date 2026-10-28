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

    /// Resolves an exchange symbol, such as `BTCUSDT`, into its base and quote
    /// assets. Returns `None` if either asset is not present in this mapping.
    pub fn trading_pair_from_symbol(&self, symbol: &str) -> Option<TradingPair> {
        let symbol = symbol.trim().to_ascii_uppercase();
        let mut resolved: Option<(usize, TradingPair)> = None;

        for (base, base_ticker) in &self.map {
            let ticker = base_ticker.0.to_ascii_uppercase();
            let Some(remainder) = symbol.strip_prefix(&ticker) else {
                continue;
            };
            let remainder = match self.symbol_separator {
                Some(separator) => match remainder.strip_prefix(separator as char) {
                    Some(remainder) => remainder,
                    None => continue,
                },
                None => remainder,
            };
            let Some((quote, _)) = self
                .map
                .iter()
                .find(|(_, quote_ticker)| quote_ticker.0.eq_ignore_ascii_case(remainder))
            else {
                continue;
            };
            if base == quote {
                continue;
            }

            let should_replace = resolved
                .as_ref()
                .is_none_or(|(length, _)| ticker.len() > *length);
            if should_replace {
                resolved = Some((ticker.len(), TradingPair::new(*base, *quote)));
            }
        }

        resolved.map(|(_, trading_pair)| trading_pair)
    }
}
