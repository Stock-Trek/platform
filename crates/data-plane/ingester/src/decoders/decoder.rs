use crate::source::ParsedMessage;
use platform_errors::IngesterError;

/// Decodes a raw exchange websocket frame into an ingester message.
pub trait FrameDecoder: Send + Sync {
    fn exchange(&self) -> &str;

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError>;
}

/// Marker for decoders that consume JSON frames.
pub trait JsonDecoder: FrameDecoder {}

/// Marker for decoders that consume binary frames.
pub trait BinaryDecoder: FrameDecoder {}
