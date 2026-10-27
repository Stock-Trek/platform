use iris::{Codec, ConnectionError};
use tokio_websockets::Message;

/// Passthrough codec that hands iris the raw websocket frame bytes.
///
/// Carrying the frame as `Vec<u8>` lets each exchange decoder choose between
/// JSON and binary payloads without iris needing to know about exchange
/// formats. The ingester never sends requests, so encoding is unsupported.
#[derive(Debug, Default, Clone, Copy)]
pub struct RawFrameCodec;

impl Codec<(), Vec<u8>> for RawFrameCodec {
    fn encode(&self, _request: &()) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError::Codec(
            "the ingester does not send websocket messages".to_string(),
        ))
    }

    fn decode(&self, message: Message) -> Result<Vec<u8>, ConnectionError> {
        if message.is_text() || message.is_binary() {
            Ok(message.into_payload().to_vec())
        } else {
            Err(ConnectionError::BadMessagePayload)
        }
    }
}
