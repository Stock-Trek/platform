use super::binance::{BinanceJsonCodec, BinanceJsonMapping, BinanceSbeCodec};
use platform_errors::IngesterError;
use platform_market_data::codec::SharedCodec;
use platform_market_data::json_mapping::{EnvelopeJsonMapping, MappedJsonCodec};
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;

pub struct CodecFactory;

impl CodecFactory {
    pub fn json_codec(exchange: CexId) -> SharedCodec {
        match exchange {
            CexId::Binance => Arc::new(BinanceJsonCodec::new(BinanceJsonMapping)),
            _ => Arc::new(MappedJsonCodec::new(EnvelopeJsonMapping)),
        }
    }

    pub fn binary_codec(exchange: CexId) -> Result<SharedCodec, IngesterError> {
        match exchange {
            CexId::Binance => Ok(Arc::new(BinanceSbeCodec)),
            _ => Err(IngesterError::UnsupportedStreamFormat {
                format: format!("binary for exchange {}", exchange),
            }),
        }
    }
}
