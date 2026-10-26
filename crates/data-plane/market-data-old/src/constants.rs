use crate::quantized_time::{Quantization, TopOfMinute};
use platform_exchange_types::urls::TradingMode;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

pub const PARQUET_SUFFIX: &str = ".parquet";
pub const LOCK_KEY: &str = "_locks/market-data-updater";

pub const UPDATE_QUANTIZATION: Quantization = Quantization::Minutes(TopOfMinute::Six);

/// Maximum number of one-minute klines an exchange will return in one request.
pub const MAX_KLINE_LIMIT: u32 = 1_000;

// Prevent a future window size from silently exceeding the exchange request limit,
// which would make every window incomplete and stall the pipeline.
const _: () = assert!(
    UPDATE_QUANTIZATION.max_window_candles() <= MAX_KLINE_LIMIT,
    "UPDATE_QUANTIZATION produces windows larger than the exchange kline limit"
);

/// Number of one-minute candles needed to fill a single update window.
pub const KLINE_LIMIT: Option<u32> = Some(UPDATE_QUANTIZATION.max_window_candles());

const COMMON_TRADING_PAIRS: &[(TradingMode, CexId, AssetId, AssetId)] = &[
    (
        TradingMode::Paper,
        CexId::Binance,
        AssetId::Bitcoin,
        AssetId::TetherUSD,
    ),
    (
        TradingMode::Paper,
        CexId::Binance,
        AssetId::Ethereum,
        AssetId::TetherUSD,
    ),
    (
        TradingMode::Real,
        CexId::Binance,
        AssetId::Bitcoin,
        AssetId::TetherUSD,
    ),
];

pub fn common_trading_pairs() -> Vec<(CexId, TradingMode, Vec<TradingPair>)> {
    let mut pairs: Vec<(CexId, TradingMode, Vec<TradingPair>)> = Vec::new();
    for (trading_mode, cex_id, base, quote) in COMMON_TRADING_PAIRS {
        let trading_pair = TradingPair::new(*base, *quote);
        match pairs
            .iter()
            .position(|(existing_cex_id, existing_trading_mode, _)| {
                existing_cex_id == cex_id && existing_trading_mode == trading_mode
            }) {
            Some(index) => pairs[index].2.push(trading_pair),
            None => pairs.push((*cex_id, *trading_mode, vec![trading_pair])),
        }
    }
    pairs
}
