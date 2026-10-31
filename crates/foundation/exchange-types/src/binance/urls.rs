use crate::urls::{TradingMode, Urls};
use platform_types::protocol::Protocol;

pub struct BinanceUrls;

impl Urls for BinanceUrls {
    fn default(&self, protocol: Protocol, trading_mode: TradingMode) -> &str {
        match protocol {
            Protocol::Http => match trading_mode {
                TradingMode::Paper => "https://testnet.binance.vision/api/v3",
                TradingMode::Real => "https://api.binance.com/api/v3",
            },
            Protocol::Websocket => match trading_mode {
                TradingMode::Paper => "wss://ws-api.testnet.binance.vision:443/ws-api/v3",
                TradingMode::Real => "wss://ws-api.binance.com:443/ws-api/v3",
            },
        }
    }
}
