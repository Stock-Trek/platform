use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketEvent {
    pub exchange: String,
    pub stream: String,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<TimestampMilliseconds>,
    pub received_at: TimestampMilliseconds,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapMarker {
    pub exchange: String,
    pub stream: String,
    pub previous_sequence: u64,
    pub observed_sequence: u64,
    pub missing_from: u64,
    pub missing_to: u64,
    pub detected_at: TimestampMilliseconds,
}
