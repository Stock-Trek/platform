use crate::source::ParsedMessage;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::IngesterError;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::{Map, Value};

/// Generic JSON stream decoder.
///
/// TODO: add exchange-specific field mappings. This decoder supports the
/// Binance-style `stream`/`data`, `e`, `E`, `s` and sequence fields and can be
/// replaced per exchange without changing the transport or ingester.
#[derive(Debug, Default)]
pub struct JsonDecoder;

impl JsonDecoder {
    pub fn new() -> Self {
        Self
    }

    pub fn decode(&self, exchange: &str, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let message = serde_json::from_slice(frame)
            .map_err(|source| IngesterError::json(exchange, source))?;
        self.decode_value(message, frame)
    }

    fn decode_value(&self, message: Value, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let (stream, data) = match message {
            Value::Object(mut object) => match object.remove("stream") {
                Some(Value::String(stream)) => {
                    let data = object
                        .remove("data")
                        .ok_or(IngesterError::MissingField("data"))?;
                    (Some(stream), data)
                }
                Some(_) => return Err(IngesterError::InvalidField("stream")),
                None => (None, Value::Object(object)),
            },
            _ => return Err(IngesterError::NotAnObject),
        };

        let object = data.as_object().ok_or(IngesterError::PayloadNotAnObject)?;
        let event_type = object.get("e").and_then(Value::as_str);
        let stream = match stream {
            Some(stream) => stream,
            None => {
                let event_type = event_type.ok_or(IngesterError::MissingField("e"))?;
                let symbol = object
                    .get("s")
                    .and_then(Value::as_str)
                    .ok_or(IngesterError::MissingField("s"))?;
                format!("{}@{event_type}", symbol.to_ascii_lowercase())
            }
        };
        let sequence = Self::parse_sequence(object, event_type)?;
        let exchange_timestamp = object
            .get("E")
            .and_then(Value::as_i64)
            .map(TimestampMilliseconds);

        Ok(ParsedMessage {
            stream,
            sequence,
            exchange_timestamp,
            payload: BASE64.encode(frame),
        })
    }

    fn parse_sequence(
        object: &Map<String, Value>,
        event_type: Option<&str>,
    ) -> Result<Option<u64>, IngesterError> {
        // TODO: validate depth updates using the `U`/`pu` range. A single depth
        // event can span many update ids, so `u` alone is not always contiguous.
        let field = match event_type {
            Some("trade") => Some("t"),
            Some("aggTrade") => Some("a"),
            Some("depthUpdate" | "bookTicker") => Some("u"),
            _ => None,
        };
        let Some(field) = field else {
            return Ok(None);
        };
        object
            .get(field)
            .and_then(Value::as_u64)
            .map(Some)
            .ok_or(IngesterError::MissingSequence { field })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_combined_json_streams() {
        let frame = serde_json::to_vec(&json!({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1_700_000_000_123_i64,
                "s": "BTCUSDT",
                "t": 42,
                "p": "100.5",
                "q": "1"
            }
        }))
        .unwrap();

        let parsed = JsonDecoder::new().decode("binance", &frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@trade");
        assert_eq!(parsed.sequence, Some(42));
        assert_eq!(
            parsed.exchange_timestamp,
            Some(TimestampMilliseconds(1_700_000_000_123))
        );
    }

    #[test]
    fn derives_a_stream_name_for_raw_json_messages() {
        let frame = serde_json::to_vec(&json!({
            "e": "depthUpdate",
            "E": 1_700_000_000_123_i64,
            "s": "BTCUSDT",
            "u": 99
        }))
        .unwrap();

        let parsed = JsonDecoder::new().decode("binance", &frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@depthUpdate");
        assert_eq!(parsed.sequence, Some(99));
    }

    #[test]
    fn json_events_without_a_sequence_are_accepted() {
        let frame = serde_json::to_vec(&json!({
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1_700_000_000_123_i64,
                "s": "BTCUSDT"
            }
        }))
        .unwrap();

        let parsed = JsonDecoder::new().decode("binance", &frame).unwrap();

        assert_eq!(parsed.sequence, None);
    }

    #[test]
    fn sequenced_json_events_must_include_their_sequence() {
        let frame = serde_json::to_vec(&json!({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "s": "BTCUSDT"
            }
        }))
        .unwrap();

        let error = JsonDecoder::new().decode("binance", &frame).unwrap_err();

        assert!(matches!(
            error,
            IngesterError::MissingSequence { field: "t" }
        ));
    }
}
