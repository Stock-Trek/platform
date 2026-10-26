pub use platform_types::protocol::Protocol;
use serde::{Deserialize, Serialize};
use strum::Display;

pub trait Urls {
    fn default(&self, protocol: Protocol, trading_mode: TradingMode) -> &str;
}

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradingMode {
    Real,
    Paper,
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
