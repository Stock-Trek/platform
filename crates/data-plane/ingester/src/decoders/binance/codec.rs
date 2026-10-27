use iris::{Codec, ConnectionError};
use tokio_websockets::Message;

/// Codec for Binance binary websocket frames.
///
/// JSON streams use iris's built-in [iris::JsonCodec]; Binance's binary streams
/// use the SBE wire format, so they get a dedicated codec rather than a generic
/// raw-frame passthrough.
#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceBinaryCodec;

impl Codec<(), BinanceBinaryFrame> for BinanceBinaryCodec {
    fn encode(&self, _request: &()) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError::Codec(
            "the ingester does not send websocket messages".to_string(),
        ))
    }

    fn decode(&self, message: Message) -> Result<BinanceBinaryFrame, ConnectionError> {
        if !message.is_binary() {
            return Err(ConnectionError::BadMessagePayload);
        }

        // TODO: validate the Binance SBE header here once multiple binary
        // message versions need to be supported.
        Ok(BinanceBinaryFrame::new(message.into_payload().to_vec()))
    }
}

/// A binary frame received from a Binance websocket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceBinaryFrame {
    payload: Vec<u8>,
}

impl BinanceBinaryFrame {
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_binary_frames() {
        let payload = vec![1, 2, 3, 4];

        let frame = BinanceBinaryCodec
            .decode(Message::binary(payload.clone()))
            .unwrap();

        assert_eq!(frame.into_bytes(), payload);
    }

    #[test]
    fn rejects_non_binary_frames() {
        let result = BinanceBinaryCodec.decode(Message::text("{}".to_string()));

        assert!(matches!(result, Err(ConnectionError::BadMessagePayload)));
    }

    #[test]
    fn encoding_requests_is_unsupported() {
        let result = BinanceBinaryCodec.encode(&());

        assert!(matches!(result, Err(ConnectionError::Codec(_))));
    }
}
