use crate::decoders::{
    binance_json::{BinanceJsonDecoder, BinanceJsonMapping},
    binance_sbe::BinanceSbeDecoder,
    decoder::{BinaryDecoder, JsonDecoder},
    json::{EnvelopeJsonMapping, MappedJsonDecoder},
};
use platform_errors::IngesterError;
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;

/// Builds exchange-specific decoders for a websocket stream format.
pub struct DecoderFactory;

impl DecoderFactory {
    /// Builds the JSON decoder for an exchange.
    ///
    /// TODO: register additional exchange mappings as they are implemented.
    pub fn json_decoder(exchange: CexId) -> Arc<dyn JsonDecoder> {
        match exchange {
            CexId::Binance => Arc::new(BinanceJsonDecoder::new(exchange, BinanceJsonMapping)),
            _ => Arc::new(MappedJsonDecoder::new(exchange, EnvelopeJsonMapping)),
        }
    }

    /// Builds the binary decoder for an exchange.
    ///
    /// TODO: register additional exchange binary decoders as they are implemented.
    pub fn binary_decoder(exchange: CexId) -> Result<Arc<dyn BinaryDecoder>, IngesterError> {
        match exchange {
            CexId::Binance => Ok(Arc::new(BinanceSbeDecoder::new(exchange))),
            _ => Err(IngesterError::UnsupportedStreamFormat {
                format: format!("binary for exchange {}", exchange),
            }),
        }
    }
}
