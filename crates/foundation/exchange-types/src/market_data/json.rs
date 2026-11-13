#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonSequenceField {
    pub event_type: &'static str,
    pub field: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonDepthSequenceFields {
    pub first: &'static str,
    pub last: &'static str,
    pub previous: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonEventSchema {
    pub event_type_field: &'static str,
    pub symbol_field: &'static str,
    pub event_time_field: &'static str,
    pub depth_event_type: &'static str,
    pub sequence_fields: &'static [JsonSequenceField],
    pub depth_sequence_fields: JsonDepthSequenceFields,
}

pub trait JsonEventSchemaDefinition {
    const SCHEMA: JsonEventSchema;
}
