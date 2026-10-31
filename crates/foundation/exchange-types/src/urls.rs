use platform_errors::ExchangeError;
pub use platform_types::protocol::Protocol;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum::Display;

pub trait Urls {
    fn default(&self, protocol: Protocol, trading_mode: TradingMode) -> &str;
}

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradingMode {
    Real,
    Paper,
}

impl FromStr for TradingMode {
    type Err = ExchangeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "real" => Ok(Self::Real),
            "paper" => Ok(Self::Paper),
            _ => Err(ExchangeError::UnsupportedTradingMode {
                trading_mode: value.to_string(),
            }),
        }
    }
}

pub struct LocalhostUrls;

impl Urls for LocalhostUrls {
    fn default(&self, protocol: Protocol, _trading_mode: TradingMode) -> &str {
        match protocol {
            Protocol::Http => "http://localhost",
            Protocol::Websocket => "ws://localhost",
        }
    }
}
