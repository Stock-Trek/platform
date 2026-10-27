use crate::decoders::{
    binance::{
        json::{BinanceJsonCodec, BinanceJsonMapping},
        sbe::BinanceSbeCodec,
    },
    decoder::SharedCodec,
    json::{EnvelopeJsonMapping, MappedJsonCodec},
};
use platform_errors::IngesterError;
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;

/// Builds exchange-specific websocket codecs for a stream format.
pub struct CodecFactory;

impl CodecFactory {
    /// Builds the JSON codec for an exchange.
    ///
    /// TODO: register additional exchange mappings as they are implemented.
    pub fn json_codec(exchange: CexId) -> SharedCodec {
        match exchange {
            CexId::Binance => Arc::new(BinanceJsonCodec::new(BinanceJsonMapping)),
            _ => Arc::new(MappedJsonCodec::new(EnvelopeJsonMapping)),
        }
    }

    /// Builds the binary codec for an exchange.
    ///
    /// TODO: register additional exchange binary codecs as they are implemented.
    pub fn binary_codec(exchange: CexId) -> Result<SharedCodec, IngesterError> {
        match exchange {
            CexId::Binance => Ok(Arc::new(BinanceSbeCodec)),
            _ => Err(IngesterError::UnsupportedStreamFormat {
                format: format!("binary for exchange {}", exchange),
            }),
        }
    }
}
