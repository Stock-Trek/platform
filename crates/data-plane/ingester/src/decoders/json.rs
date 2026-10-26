use crate::{
    decoders::decoder::{FrameDecoder, JsonDecoder},
    source::ParsedMessage,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::IngesterError;
use serde_json::{Map, Value};
use stock_trek_types::cex::cex_id::CexId;

/// Exchange-specific interpretation of a decoded JSON websocket message.
///
/// Implementations are responsible for identifying the stream, sequence and
/// exchange timestamp. Envelope parsing and payload encoding stay in
/// [MappedJsonDecoder] so implementations only describe their own fields.
pub trait JsonMapping: Send + Sync + 'static {
    fn map(
        &self,
        exchange: CexId,
        message: Value,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError>;
}

/// Generic JSON websocket decoder driven by an exchange-specific [JsonMapping].
pub struct MappedJsonDecoder<M> {
    exchange: CexId,
    mapping: M,
}

impl<M: JsonMapping> MappedJsonDecoder<M> {
    pub fn new(exchange: CexId, mapping: M) -> Self {
        Self { exchange, mapping }
    }
}

impl<M: JsonMapping> FrameDecoder for MappedJsonDecoder<M> {
    fn exchange(&self) -> CexId {
        self.exchange
    }

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let message = serde_json::from_slice(frame)
            .map_err(|source| IngesterError::json(self.exchange.to_string(), source))?;
        self.mapping.map(self.exchange, message, frame)
    }
}

impl<M: JsonMapping> JsonDecoder for MappedJsonDecoder<M> {}

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

/// Minimal mapping for exchanges that only expose the common stream envelope.
///
/// TODO: add exchange-specific mappings for sequences and timestamps.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvelopeJsonMapping;

impl JsonMapping for EnvelopeJsonMapping {
    fn map(
        &self,
        _exchange: CexId,
        message: Value,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let (stream, _data) = JsonFrame::split_envelope(message)?;
        let stream = stream.ok_or(IngesterError::MissingField("stream"))?;
        Ok(ParsedMessage {
            stream,
            sequence: None,
            exchange_timestamp: None,
            payload: JsonFrame::encode(frame),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn envelope_mapping_extracts_the_stream() {
        let frame = serde_json::to_vec(&json!({
            "stream": "btcusdt@trade",
            "data": { "e": "trade" }
        }))
        .unwrap();
        let decoder = MappedJsonDecoder::new(CexId::Binance, EnvelopeJsonMapping);

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@trade");
        assert_eq!(parsed.sequence, None);
        assert_eq!(parsed.exchange_timestamp, None);
    }

    #[test]
    fn envelope_mapping_requires_a_stream() {
        let frame = serde_json::to_vec(&json!({ "e": "trade" })).unwrap();
        let decoder = MappedJsonDecoder::new(CexId::Binance, EnvelopeJsonMapping);

        let error = decoder.decode(&frame).unwrap_err();

        assert!(matches!(error, IngesterError::MissingField("stream")));
    }
}
