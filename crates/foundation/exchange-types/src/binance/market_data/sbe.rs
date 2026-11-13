use crate::market_data::{
    SbeBestBidAskRecord, SbeSchema, SbeSchemaDefinition, SbeTemplate, SbeTemplateKind,
    SbeTradeRecord,
};
use rust_decimal::Decimal;

#[derive(Debug, Default, Clone, Copy)]
pub struct BinanceSbeSchema;

#[derive(Debug, Clone, PartialEq)]
pub struct BinanceSbeTrade {
    pub symbol: String,
    pub event_time_us: i64,
    pub price: Decimal,
    pub quantity: Decimal,
}

impl SbeTradeRecord for BinanceSbeTrade {
    fn new(symbol: String, event_time_us: i64, price: Decimal, quantity: Decimal) -> Self {
        Self {
            symbol,
            event_time_us,
            price,
            quantity,
        }
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn event_time_us(&self) -> i64 {
        self.event_time_us
    }

    fn price(&self) -> Decimal {
        self.price
    }

    fn quantity(&self) -> Decimal {
        self.quantity
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinanceSbeBestBidAsk {
    pub symbol: String,
    pub event_time_us: i64,
    pub bid_price: Decimal,
    pub bid_quantity: Decimal,
    pub ask_price: Decimal,
    pub ask_quantity: Decimal,
}

impl SbeBestBidAskRecord for BinanceSbeBestBidAsk {
    fn new(
        symbol: String,
        event_time_us: i64,
        bid_price: Decimal,
        bid_quantity: Decimal,
        ask_price: Decimal,
        ask_quantity: Decimal,
    ) -> Self {
        Self {
            symbol,
            event_time_us,
            bid_price,
            bid_quantity,
            ask_price,
            ask_quantity,
        }
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn event_time_us(&self) -> i64 {
        self.event_time_us
    }

    fn bid_price(&self) -> Decimal {
        self.bid_price
    }

    fn bid_quantity(&self) -> Decimal {
        self.bid_quantity
    }

    fn ask_price(&self) -> Decimal {
        self.ask_price
    }

    fn ask_quantity(&self) -> Decimal {
        self.ask_quantity
    }
}

impl BinanceSbeSchema {
    pub const SCHEMA_ID: u16 = 1;
    pub const VERSION: u16 = 0;

    pub const TRADES_TEMPLATE_ID: u16 = 10_000;
    pub const BEST_BID_ASK_TEMPLATE_ID: u16 = 10_001;
    pub const DEPTH_SNAPSHOT_TEMPLATE_ID: u16 = 10_002;
    pub const DEPTH_DIFF_TEMPLATE_ID: u16 = 10_003;

    pub const TRADES_BLOCK_LENGTH: u16 = 18;
    pub const BEST_BID_ASK_BLOCK_LENGTH: u16 = 50;
    pub const DEPTH_SNAPSHOT_BLOCK_LENGTH: u16 = 18;
    pub const DEPTH_DIFF_BLOCK_LENGTH: u16 = 26;
}

impl SbeSchemaDefinition for BinanceSbeSchema {
    type Trade = BinanceSbeTrade;
    type BestBidAsk = BinanceSbeBestBidAsk;

    const SCHEMA: SbeSchema = SbeSchema {
        schema_id: Self::SCHEMA_ID,
        version: Self::VERSION,
        templates: &[
            SbeTemplate {
                template_id: Self::TRADES_TEMPLATE_ID,
                block_length: Self::TRADES_BLOCK_LENGTH,
                kind: SbeTemplateKind::Trades,
            },
            SbeTemplate {
                template_id: Self::BEST_BID_ASK_TEMPLATE_ID,
                block_length: Self::BEST_BID_ASK_BLOCK_LENGTH,
                kind: SbeTemplateKind::BestBidAsk,
            },
            SbeTemplate {
                template_id: Self::DEPTH_SNAPSHOT_TEMPLATE_ID,
                block_length: Self::DEPTH_SNAPSHOT_BLOCK_LENGTH,
                kind: SbeTemplateKind::DepthSnapshot,
            },
            SbeTemplate {
                template_id: Self::DEPTH_DIFF_TEMPLATE_ID,
                block_length: Self::DEPTH_DIFF_BLOCK_LENGTH,
                kind: SbeTemplateKind::DepthDiff,
            },
        ],
    };
}
