pub mod binance_json;
pub mod binance_sbe;
pub mod json;

pub use binance_json::{BinanceJsonDecoder, BinanceJsonMapping};
pub use binance_sbe::BinanceSbeDecoder;
pub use json::{EnvelopeJsonMapping, JsonMapping, MappedJsonDecoder};

use crate::source::ParsedMessage;
use platform_errors::IngesterError;
use std::sync::Arc;

/// Decodes a raw exchange websocket frame into an ingester message.
pub trait FrameDecoder: Send + Sync {
    fn exchange(&self) -> &str;

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError>;
}

/// Marker for decoders that consume JSON frames.
pub trait JsonDecoder: FrameDecoder {}

/// Marker for decoders that consume binary frames.
pub trait BinaryDecoder: FrameDecoder {}

/// Builds the JSON decoder for an exchange.
///
/// TODO: register additional exchange mappings as they are implemented.
pub fn json_decoder(exchange: &str) -> Arc<dyn JsonDecoder> {
    match exchange.to_ascii_lowercase().as_str() {
        "binance" => Arc::new(BinanceJsonDecoder::new(exchange, BinanceJsonMapping)),
        _ => Arc::new(MappedJsonDecoder::new(exchange, EnvelopeJsonMapping)),
    }
}

/// Builds the binary decoder for an exchange.
///
/// TODO: register additional exchange binary decoders as they are implemented.
pub fn binary_decoder(exchange: &str) -> Result<Arc<dyn BinaryDecoder>, IngesterError> {
    match exchange.to_ascii_lowercase().as_str() {
        "binance" => Ok(Arc::new(BinanceSbeDecoder::new(exchange))),
        exchange => Err(IngesterError::UnsupportedStreamFormat {
            format: format!("binary for exchange {exchange}"),
        }),
    }
}
