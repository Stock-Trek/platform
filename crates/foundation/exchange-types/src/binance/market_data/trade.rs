use crate::{
    binance::market_data::sbe::BinanceSbeSchema,
    market_data::{TradeSchema, TradeSchemaDefinition},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceTradeSchema;

impl TradeSchemaDefinition for BinanceTradeSchema {
    type Sbe = BinanceSbeSchema;

    const SCHEMA: TradeSchema = TradeSchema {
        stream_suffixes: &["@trade", "@aggtrade"],
        event_types: &["trade", "aggtrade"],
        event_type_field: "e",
        symbol_field: "s",
        price_field: "p",
        quantity_field: "q",
        timestamp_fields: &["T", "E"],
    };
}
