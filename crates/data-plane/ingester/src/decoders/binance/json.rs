use crate::{
    decoders::json::{JsonFrame, JsonMapping, MappedJsonCodec},
    source::ParsedMessage,
};
use platform_errors::IngesterError;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::{Map, Value};

const EVENT_TYPE: &str = "e";
const SYMBOL: &str = "s";
const EVENT_TIME: &str = "E";

/// Binance event types whose sequence is carried by a dedicated field.
const SEQUENCE_FIELDS: &[(&str, &str)] = &[
    ("trade", "t"),
    ("aggTrade", "a"),
    ("depthUpdate", "u"),
    ("bookTicker", "u"),
];

/// Binance JSON websocket codec.
pub type BinanceJsonCodec = MappedJsonCodec<BinanceJsonMapping>;

/// Binance-specific field mapping for JSON websocket streams.
#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceJsonMapping;

impl JsonMapping for BinanceJsonMapping {
    fn map(&self, message: Value, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let (stream, data) = JsonFrame::split_envelope(message)?;
        let event_type = data.get(EVENT_TYPE).and_then(Value::as_str);
        let stream = match stream {
            Some(stream) => stream,
            None => {
                let event_type = event_type.ok_or(IngesterError::MissingField(EVENT_TYPE))?;
                let symbol = data
                    .get(SYMBOL)
                    .and_then(Value::as_str)
                    .ok_or(IngesterError::MissingField(SYMBOL))?;
                format!("{}@{event_type}", symbol.to_ascii_lowercase())
            }
        };

        Ok(ParsedMessage {
            stream,
            sequence: self.sequence(event_type, &data)?,
            exchange_timestamp: data
                .get(EVENT_TIME)
                .and_then(Value::as_i64)
                .map(TimestampMilliseconds),
            payload: JsonFrame::encode(frame),
        })
    }
}

impl BinanceJsonMapping {
    fn sequence(
        &self,
        event_type: Option<&str>,
        data: &Map<String, Value>,
    ) -> Result<Option<u64>, IngesterError> {
        // TODO: validate depth updates using the `U`/`pu` range. A single depth
        // event can span many update ids, so `u` alone is not always contiguous.
        let Some((_, field)) = SEQUENCE_FIELDS
            .iter()
            .find(|(candidate, _)| Some(*candidate) == event_type)
        else {
            return Ok(None);
        };
        data.get(*field)
            .and_then(Value::as_u64)
            .map(Some)
            .ok_or(IngesterError::MissingSequence { field })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use iris::Codec;
    use serde_json::json;
    use tokio_websockets::Message;

    fn codec() -> BinanceJsonCodec {
        BinanceJsonCodec::new(BinanceJsonMapping)
    }

    fn decode(frame: Vec<u8>) -> Result<ParsedMessage, IngesterError> {
        codec().decode(Message::binary(frame)).unwrap().result
    }

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

        let parsed = decode(frame.clone()).unwrap();

        assert_eq!(parsed.stream, "btcusdt@trade");
        assert_eq!(parsed.sequence, Some(42));
        assert_eq!(
            parsed.exchange_timestamp,
            Some(TimestampMilliseconds(1_700_000_000_123))
        );
        assert_eq!(BASE64.decode(&parsed.payload).unwrap(), frame);
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

        let parsed = decode(frame).unwrap();

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

        let parsed = decode(frame).unwrap();

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

        let error = decode(frame).unwrap_err();

        assert!(matches!(
            error,
            IngesterError::MissingSequence { field: "t" }
        ));
    }
}
