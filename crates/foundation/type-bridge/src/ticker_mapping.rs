use bimap::BiMap;
use platform_exchange_types::ticker::Ticker;
use std::collections::HashMap;
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

#[derive(Debug, Clone)]
pub struct TickerMapping {
    pub map: BiMap<AssetId, Ticker>,
    pub symbol_separator: Option<u8>,
    /// Precomputed lookup from an uppercase exchange symbol to its trading pair.
    symbol_to_pair: HashMap<String, TradingPair>,
}

impl TickerMapping {
    pub fn new(map: BiMap<AssetId, Ticker>, symbol_separator: Option<u8>) -> Self {
        let mut tickers = map
            .iter()
            .map(|(asset_id, ticker)| (ticker.0.to_ascii_uppercase(), *asset_id))
            .collect::<Vec<_>>();
        // Consider longer base tickers first so ambiguous symbols resolve to the
        // same pair as the previous longest-prefix search.
        tickers.sort_by_key(|(ticker, _)| std::cmp::Reverse(ticker.len()));

        let mut symbol_to_pair = HashMap::with_capacity(tickers.len() * tickers.len());
        for (base_ticker, base) in &tickers {
            for (quote_ticker, quote) in &tickers {
                if base == quote {
                    continue;
                }
                let symbol = match symbol_separator {
                    Some(separator) => {
                        format!("{base_ticker}{}{quote_ticker}", separator as char)
                    }
                    None => format!("{base_ticker}{quote_ticker}"),
                };
                symbol_to_pair
                    .entry(symbol)
                    .or_insert_with(|| TradingPair::new(*base, *quote));
            }
        }

        Self {
            map,
            symbol_separator,
            symbol_to_pair,
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
        self.symbol_to_pair
            .get(&symbol.trim().to_ascii_uppercase())
            .copied()
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
        let mut map = BiHashMap::new();
        map.insert(AssetId::Bitcoin, "B".into());
        map.insert(AssetId::BitcoinCash, "BCH".into());
        map.insert(AssetId::TetherUSD, "USDT".into());
        let mapping = TickerMapping::new(map, None);

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
