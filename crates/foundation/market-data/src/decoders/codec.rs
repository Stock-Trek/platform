use crate::decoders::{
    frame::DecodedFrame, json_mapping::MappedJsonCodec, json_schema_mapping::SchemaJsonMapping,
    sbe::SbeCodec,
};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use platform_exchange_types::binance::market_data::{BinanceJsonSchema, BinanceSbeSchema};
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;
use tokio_websockets::Message;

pub type SharedCodec = Arc<dyn Codec<(), DecodedFrame>>;

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
            CexId::Binance => Arc::new(MappedJsonCodec::new(
                SchemaJsonMapping::<BinanceJsonSchema>::default(),
            )),
            _ => Arc::new(MappedJsonCodec::new(
                crate::decoders::json_mapping::EnvelopeJsonMapping,
            )),
        }
    }

    pub fn binary_codec(exchange: CexId) -> Result<SharedCodec, IngesterError> {
        match exchange {
            CexId::Binance => Ok(Arc::new(SbeCodec::<BinanceSbeSchema>::new())),
            _ => Err(IngesterError::UnsupportedStreamFormat {
                format: format!("binary for exchange {}", exchange),
            }),
        }
    }
}
