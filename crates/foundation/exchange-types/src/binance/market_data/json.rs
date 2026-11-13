use crate::market_data::{
    JsonDepthSequenceFields, JsonEventSchema, JsonEventSchemaDefinition, JsonSequenceField,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceJsonSchema;

impl JsonEventSchemaDefinition for BinanceJsonSchema {
    const SCHEMA: JsonEventSchema = JsonEventSchema {
        event_type_field: "e",
        symbol_field: "s",
        event_time_field: "E",
        depth_event_type: "depthUpdate",
        sequence_fields: &[
            JsonSequenceField {
                event_type: "trade",
                field: "t",
            },
            JsonSequenceField {
                event_type: "aggTrade",
                field: "a",
            },
            JsonSequenceField {
                event_type: "bookTicker",
                field: "u",
            },
        ],
        depth_sequence_fields: JsonDepthSequenceFields {
            first: "U",
            last: "u",
            previous: "pu",
        },
    };
}
