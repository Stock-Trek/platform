use crate::{
    decoders::frame::{DecodedFrame, FrameBytes},
    event::ParsedMessage,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use serde_json::{Map, Value};
use tokio_websockets::Message;

/// Exchange-specific interpretation of a decoded JSON websocket message.
///
/// Implementations are responsible for identifying the stream, sequence and
/// exchange timestamp. Envelope parsing, frame encoding and transport concerns
/// stay in [MappedJsonCodec] so implementations only describe their own fields.
pub trait JsonMapping: Send + Sync + 'static {
    fn map(&self, message: Value, frame: &[u8]) -> Result<ParsedMessage, IngesterError>;
}

/// Generic JSON websocket codec driven by an exchange-specific [JsonMapping].
pub struct MappedJsonCodec<M> {
    mapping: M,
}

impl<M: JsonMapping> MappedJsonCodec<M> {
    pub fn new(mapping: M) -> Self {
        Self { mapping }
    }
}

impl<M: JsonMapping> Codec<(), DecodedFrame> for MappedJsonCodec<M> {
    fn encode(&self, _request: &()) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError::Codec(
            "the ingester does not send websocket messages".to_string(),
        ))
    }

    fn decode(&self, message: Message) -> Result<DecodedFrame, ConnectionError> {
        let frame = FrameBytes::from_message(message)?;
        let result = serde_json::from_slice(&frame)
            .map_err(IngesterError::json)
            .and_then(|message| self.mapping.map(message, &frame));
        Ok(DecodedFrame::new(result))
    }
}

/// Helpers shared by JSON mappings for common websocket frame shapes.
pub(crate) struct JsonFrame;

impl JsonFrame {
    /// Splits the common `{"stream": ..., "data": ...}` envelope.
    pub(crate) fn split_envelope(
        message: Value,
    ) -> Result<(Option<String>, Map<String, Value>), IngesterError> {
        match message {
            Value::Object(mut object) => match object.remove("stream") {
                Some(Value::String(stream)) => {
                    let data = object
                        .remove("data")
                        .ok_or(IngesterError::MissingField("data"))?;
                    let data = data
                        .as_object()
                        .cloned()
                        .ok_or(IngesterError::PayloadNotAnObject)?;
                    Ok((Some(stream), data))
                }
                Some(_) => Err(IngesterError::InvalidField("stream")),
                None => Ok((None, object)),
            },
            _ => Err(IngesterError::NotAnObject),
        }
    }

    pub(crate) fn encode(frame: &[u8]) -> String {
        BASE64.encode(frame)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EnvelopeJsonMapping;

impl JsonMapping for EnvelopeJsonMapping {
    fn map(&self, message: Value, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let (stream, _data) = JsonFrame::split_envelope(message)?;
        let stream = stream.ok_or(IngesterError::MissingField("stream"))?;
        Ok(ParsedMessage {
            stream,
            sequence: None,
            first_sequence: None,
            previous_sequence: None,
            exchange_timestamp: None,
            payload: JsonFrame::encode(frame),
        })
    }
}
