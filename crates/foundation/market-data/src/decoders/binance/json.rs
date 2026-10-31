use crate::{
    decoders::json::{JsonFrame, JsonMapping, MappedJsonCodec},
    event::ParsedMessage,
};
use platform_errors::IngesterError;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::{Map, Value};

const EVENT_TYPE: &str = "e";
const SYMBOL: &str = "s";
const EVENT_TIME: &str = "E";
const DEPTH_EVENT_TYPE: &str = "depthUpdate";

/// Binance event types whose sequence is carried by a dedicated field.
const SEQUENCE_FIELDS: &[(&str, &str)] = &[("trade", "t"), ("aggTrade", "a"), ("bookTicker", "u")];

/// Sequence metadata extracted from a Binance JSON event.
#[derive(Debug, Default, Clone, Copy)]
struct Sequences {
    last: Option<u64>,
    first: Option<u64>,
    previous: Option<u64>,
}

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

        let Sequences {
            last: sequence,
            first: first_sequence,
            previous: previous_sequence,
        } = self.sequences(event_type, &data)?;

        Ok(ParsedMessage {
            stream,
            sequence,
            first_sequence,
            previous_sequence,
            exchange_timestamp: data
                .get(EVENT_TIME)
                .and_then(Value::as_i64)
                .map(TimestampMilliseconds),
            payload: JsonFrame::encode(frame),
        })
    }
}

impl BinanceJsonMapping {
    fn sequences(
        &self,
        event_type: Option<&str>,
        data: &Map<String, Value>,
    ) -> Result<Sequences, IngesterError> {
        if event_type == Some(DEPTH_EVENT_TYPE) {
            return self.depth_sequences(data);
        }

        let Some((_, field)) = SEQUENCE_FIELDS
            .iter()
            .find(|(candidate, _)| Some(*candidate) == event_type)
        else {
            return Ok(Sequences::default());
        };

        Ok(Sequences {
            last: Some(Self::required_sequence(data, field)?),
            ..Sequences::default()
        })
    }

    /// Extracts the update-ID range from a depth update.
    ///
    /// Spot depth events only carry `U` (first update ID) and `u` (final
    /// update ID). Futures depth events also carry `pu`, the final update ID
    /// of the preceding event. `pu` is the authoritative continuity check for
    /// futures streams: consecutive events must have `pu == previous u`, even
    /// when `U` skips update IDs that did not change the book.
    fn depth_sequences(&self, data: &Map<String, Value>) -> Result<Sequences, IngesterError> {
        let first = Self::required_sequence(data, "U")?;
        let last = Self::required_sequence(data, "u")?;
        if first > last {
            return Err(IngesterError::InvalidSequence {
                field: "U",
                reason: "must not exceed the final update ID `u`",
            });
        }

        let previous = match data.get("pu") {
            Some(value) => {
                let previous = value.as_u64().ok_or(IngesterError::InvalidField("pu"))?;
                if previous >= first {
                    return Err(IngesterError::InvalidSequence {
                        field: "pu",
                        reason: "must be less than the first update ID `U`",
                    });
                }
                Some(previous)
            }
            None => None,
        };

        Ok(Sequences {
            last: Some(last),
            first: Some(first),
            previous,
        })
    }

    fn required_sequence(
        data: &Map<String, Value>,
        field: &'static str,
    ) -> Result<u64, IngesterError> {
        data.get(field)
            .and_then(Value::as_u64)
            .ok_or(IngesterError::MissingSequence { field })
    }
}

#[cfg(test)]
mod tests;
