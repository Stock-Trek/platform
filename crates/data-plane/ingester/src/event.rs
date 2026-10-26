use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketEvent {
    pub exchange: String,
    pub stream: String,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<TimestampMilliseconds>,
    pub received_at: TimestampMilliseconds,
    /// Base64 encoded raw frame from the exchange. Keeping the original bytes
    /// lets downstream consumers decode exchange-specific binary formats.
    pub payload: String,
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
