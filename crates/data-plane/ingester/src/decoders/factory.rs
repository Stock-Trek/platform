use crate::decoders::{
    binance_json::{BinanceJsonDecoder, BinanceJsonMapping},
    binance_sbe::BinanceSbeDecoder,
    decoder::{BinaryDecoder, JsonDecoder},
    json::{EnvelopeJsonMapping, MappedJsonDecoder},
};
use platform_errors::IngesterError;
use std::sync::Arc;

/// Builds exchange-specific decoders for a websocket stream format.
pub struct DecoderFactory;

impl DecoderFactory {
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
}
