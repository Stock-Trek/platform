use crate::event::ParsedMessage;
use iris::ConnectionError;
use platform_errors::IngesterError;
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
