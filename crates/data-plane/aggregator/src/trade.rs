use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

/// A single exchange trade used to build candles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: CexId,
    pub symbol: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: TimestampMilliseconds,
}
