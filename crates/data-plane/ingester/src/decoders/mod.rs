pub mod binance_sbe;
pub mod json;

pub use binance_sbe::BinanceSbeDecoder;
pub use json::JsonDecoder;

use crate::source::ParsedMessage;
use platform_errors::IngesterError;

/// Decodes exchange-specific binary websocket frames.
pub trait BinaryDecoder: Send + Sync {
    fn exchange(&self) -> &str;

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError>;
}
