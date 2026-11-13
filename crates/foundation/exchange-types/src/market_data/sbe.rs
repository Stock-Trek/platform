use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SbeTemplateKind {
    Trades,
    BestBidAsk,
    DepthSnapshot,
    DepthDiff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SbeTemplate {
    pub template_id: u16,
    pub block_length: u16,
    pub kind: SbeTemplateKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SbeSchema {
    pub schema_id: u16,
    pub version: u16,
    pub templates: &'static [SbeTemplate],
}

pub trait SbeTradeRecord {
    fn new(symbol: String, event_time_us: i64, price: Decimal, quantity: Decimal) -> Self;
    fn symbol(&self) -> &str;
    fn event_time_us(&self) -> i64;
    fn price(&self) -> Decimal;
    fn quantity(&self) -> Decimal;
}

pub trait SbeBestBidAskRecord {
    #[allow(clippy::too_many_arguments)]
    fn new(
        symbol: String,
        event_time_us: i64,
        bid_price: Decimal,
        bid_quantity: Decimal,
        ask_price: Decimal,
        ask_quantity: Decimal,
    ) -> Self;
    fn symbol(&self) -> &str;
    fn event_time_us(&self) -> i64;
    fn bid_price(&self) -> Decimal;
    fn bid_quantity(&self) -> Decimal;
    fn ask_price(&self) -> Decimal;
    fn ask_quantity(&self) -> Decimal;
}

pub trait SbeSchemaDefinition {
    type Trade: SbeTradeRecord;
    type BestBidAsk: SbeBestBidAskRecord;
    const SCHEMA: SbeSchema;
}
