use crate::market_data::sbe::SbeSchemaDefinition;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeSchema {
    pub stream_suffixes: &'static [&'static str],
    pub event_types: &'static [&'static str],
    pub event_type_field: &'static str,
    pub symbol_field: &'static str,
    pub price_field: &'static str,
    pub quantity_field: &'static str,
    pub timestamp_fields: &'static [&'static str],
}

pub trait TradeSchemaDefinition {
    type Sbe: SbeSchemaDefinition;
    const SCHEMA: TradeSchema;
}
