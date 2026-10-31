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
