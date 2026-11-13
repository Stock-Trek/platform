use crate::decoders::frame::DecodedFrame;
use iris::{Codec, ConnectionError};
use std::sync::Arc;
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
