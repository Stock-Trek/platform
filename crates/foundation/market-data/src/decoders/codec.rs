use crate::decoders::{
    binance::{
        json::{BinanceJsonCodec, BinanceJsonMapping},
        sbe::BinanceSbeCodec,
    },
    frame::DecodedFrame,
    json::{EnvelopeJsonMapping, MappedJsonCodec},
};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;
use tokio_websockets::Message;

pub type SharedCodec = Arc<dyn Codec<(), DecodedFrame>>;

/// Wraps a shared codec so iris, which takes codecs by value, can use it.
#[derive(Clone)]
pub struct CodecHandle {
    codec: SharedCodec,
}

impl CodecHandle {
    pub fn new(codec: SharedCodec) -> Self {
        Self { codec }
    }
}

impl Codec<(), DecodedFrame> for CodecHandle {
    fn encode(&self, request: &()) -> Result<Vec<u8>, ConnectionError> {
        self.codec.encode(request)
    }

    fn decode(&self, message: Message) -> Result<DecodedFrame, ConnectionError> {
        self.codec.decode(message)
    }
}

impl std::fmt::Debug for CodecHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CodecHandle").finish()
    }
}

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
