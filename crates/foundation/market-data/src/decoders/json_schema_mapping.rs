use crate::{
    decoders::json_mapping::{JsonFrame, JsonMapping, MappedJsonCodec},
    event::ParsedMessage,
};
use platform_errors::IngesterError;
use platform_exchange_types::market_data::{JsonEventSchema, JsonEventSchemaDefinition};
use platform_types::TimestampMilliseconds;
use serde_json::{Map, Value};
use std::marker::PhantomData;

pub type SchemaJsonCodec<S> = MappedJsonCodec<SchemaJsonMapping<S>>;

pub struct SchemaJsonMapping<S> {
    _schema: PhantomData<S>,
}

impl<S> SchemaJsonMapping<S> {
    pub const fn new() -> Self {
        Self {
            _schema: PhantomData,
        }
    }
}

impl<S> Default for SchemaJsonMapping<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Clone for SchemaJsonMapping<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for SchemaJsonMapping<S> {}

impl<S: JsonEventSchemaDefinition + Send + Sync + 'static> JsonMapping for SchemaJsonMapping<S> {
    fn map(&self, message: Value, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let schema = S::SCHEMA;
        let (stream, data) = JsonFrame::split_envelope(message)?;
        let event_type = data.get(schema.event_type_field).and_then(Value::as_str);
        let stream = match stream {
            Some(stream) => stream,
            None => {
                let event_type =
                    event_type.ok_or(IngesterError::MissingField(schema.event_type_field))?;
                let symbol = data
                    .get(schema.symbol_field)
                    .and_then(Value::as_str)
                    .ok_or(IngesterError::MissingField(schema.symbol_field))?;
                format!("{}@{event_type}", symbol.to_ascii_lowercase())
            }
        };
        let sequences = Self::sequences(&schema, event_type, &data)?;

        Ok(ParsedMessage {
            stream,
            sequence: sequences.last,
            first_sequence: sequences.first,
            previous_sequence: sequences.previous,
            exchange_timestamp: data
                .get(schema.event_time_field)
                .and_then(Value::as_i64)
                .map(TimestampMilliseconds),
            payload: JsonFrame::encode(frame),
        })
    }
}

impl<S: JsonEventSchemaDefinition + Send + Sync + 'static> SchemaJsonMapping<S> {
    fn sequences(
        schema: &JsonEventSchema,
        event_type: Option<&str>,
        data: &Map<String, Value>,
    ) -> Result<Sequences, IngesterError> {
        if event_type == Some(schema.depth_event_type) {
            return Self::depth_sequences(schema, data);
        }

        let Some(sequence) = schema
            .sequence_fields
            .iter()
            .find(|candidate| Some(candidate.event_type) == event_type)
        else {
            return Ok(Sequences::default());
        };

        Ok(Sequences {
            last: Some(Self::required_sequence(data, sequence.field)?),
            ..Sequences::default()
        })
    }

    fn depth_sequences(
        schema: &JsonEventSchema,
        data: &Map<String, Value>,
    ) -> Result<Sequences, IngesterError> {
        let fields = schema.depth_sequence_fields;
        let first = Self::required_sequence(data, fields.first)?;
        let last = Self::required_sequence(data, fields.last)?;
        if first > last {
            return Err(IngesterError::InvalidSequence {
                field: fields.first,
                reason: "must not exceed the final update ID",
            });
        }

        let previous = match data.get(fields.previous) {
            Some(value) => {
                let previous = value
                    .as_u64()
                    .ok_or(IngesterError::InvalidField(fields.previous))?;
                if previous >= first {
                    return Err(IngesterError::InvalidSequence {
                        field: fields.previous,
                        reason: "must be less than the first update ID",
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

#[derive(Debug, Default, Clone, Copy)]
struct Sequences {
    last: Option<u64>,
    first: Option<u64>,
    previous: Option<u64>,
}
