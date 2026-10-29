use bimap::BiMap;
use platform_exchange_types::ticker::Ticker;
use std::collections::HashMap;
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

#[derive(Debug, Clone)]
pub struct TickerMapping {
    pub map: BiMap<AssetId, Ticker>,
    pub symbol_separator: Option<u8>,
    /// Uppercase tickers paired with their asset, used to resolve symbols
    /// without reallocating on every lookup.
    tickers_by_upper: Vec<(String, AssetId)>,
    assets_by_upper_ticker: HashMap<String, AssetId>,
}

impl TickerMapping {
    pub fn new(map: BiMap<AssetId, Ticker>, symbol_separator: Option<u8>) -> Self {
        let tickers_by_upper = map
            .iter()
            .map(|(asset_id, ticker)| (ticker.0.to_ascii_uppercase(), *asset_id))
            .collect::<Vec<_>>();
        let assets_by_upper_ticker = tickers_by_upper
            .iter()
            .map(|(ticker, asset_id)| (ticker.clone(), *asset_id))
            .collect();

        Self {
            map,
            symbol_separator,
            tickers_by_upper,
            assets_by_upper_ticker,
        }
    }

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

        for (base_ticker, base) in &self.tickers_by_upper {
            let Some(remainder) = symbol.strip_prefix(base_ticker) else {
                continue;
            };
            let remainder = match self.symbol_separator {
                Some(separator) => match remainder.strip_prefix(separator as char) {
                    Some(remainder) => remainder,
                    None => continue,
                },
                None => remainder,
            };
            let Some(quote) = self.assets_by_upper_ticker.get(remainder) else {
                continue;
            };
            if base == quote {
                continue;
            }

            let should_replace = resolved
                .as_ref()
                .is_none_or(|(length, _)| base_ticker.len() > *length);
            if should_replace {
                resolved = Some((base_ticker.len(), TradingPair::new(*base, *quote)));
            }
        }

        resolved.map(|(_, trading_pair)| trading_pair)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bimap::BiHashMap;

    fn mapping(symbol_separator: Option<u8>) -> TickerMapping {
        let mut map = BiHashMap::new();
        map.insert(AssetId::Bitcoin, "BTC".into());
        map.insert(AssetId::BitcoinCash, "BCH".into());
        map.insert(AssetId::Ethereum, "ETH".into());
        map.insert(AssetId::TetherUSD, "USDT".into());
        TickerMapping::new(map, symbol_separator)
    }

    #[test]
    fn resolves_concatenated_symbols_case_insensitively() {
        let mapping = mapping(None);

        assert_eq!(
            mapping.trading_pair_from_symbol("btcusdt"),
            Some(TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD))
        );
        assert_eq!(
            mapping.trading_pair_from_symbol(" ethusdt "),
            Some(TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD))
        );
    }

    #[test]
    fn prefers_the_longest_matching_base_ticker() {
        let mapping = mapping(None);

        assert_eq!(
            mapping.trading_pair_from_symbol("BCHUSDT"),
            Some(TradingPair::new(AssetId::BitcoinCash, AssetId::TetherUSD))
        );
    }

    #[test]
    fn resolves_separated_symbols() {
        let mapping = mapping(Some(b'-'));

        assert_eq!(
            mapping.trading_pair_from_symbol("BTC-USDT"),
            Some(TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD))
        );
        assert_eq!(mapping.trading_pair_from_symbol("BTCUSDT"), None);
    }

    #[test]
    fn returns_none_for_unknown_symbols() {
        let mapping = mapping(None);

        assert_eq!(mapping.trading_pair_from_symbol("DOGEUSDT"), None);
        assert_eq!(mapping.trading_pair_from_symbol("BTC"), None);
    }
}
