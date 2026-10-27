use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

/// A market data event published by the ingester.
///
/// The payload is the original base64 encoded exchange frame. The aggregator
/// decodes it separately so this envelope stays independent of exchange
/// formats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketEvent {
    pub exchange: CexId,
    pub stream: String,
    pub exchange_timestamp: Option<TimestampMilliseconds>,
    pub received_at: TimestampMilliseconds,
    pub payload: String,
}
