use crate::source::ParsedMessage;
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use std::{fmt, sync::Arc};
use tokio_websockets::Message;

/// Result of decoding a single exchange websocket frame.
///
/// Decode failures are carried in the result instead of failing the codec so a
/// malformed exchange frame does not tear down the websocket connection.
#[derive(Debug)]
pub struct DecodedFrame {
    pub result: Result<ParsedMessage, IngesterError>,
}

impl DecodedFrame {
    pub fn new(result: Result<ParsedMessage, IngesterError>) -> Self {
        Self { result }
    }
}

/// Type-erased codec for an exchange websocket stream.
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

impl fmt::Debug for CodecHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CodecHandle").finish()
    }
}

/// Extracts the raw payload bytes from an iris websocket message.
pub struct FrameBytes;

impl FrameBytes {
    pub fn from_message(message: Message) -> Result<Vec<u8>, ConnectionError> {
        if message.is_text() {
            message
                .as_text()
                .map(|text| text.as_bytes().to_vec())
                .ok_or(ConnectionError::BadMessagePayload)
        } else if message.is_binary() {
            Ok(message.into_payload().to_vec())
        } else {
            Err(ConnectionError::BadMessagePayload)
        }
    }
}
