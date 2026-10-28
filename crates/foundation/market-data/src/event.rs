use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

/// A decoded exchange websocket frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMessage {
    pub stream: String,
    /// The final sequence number carried by the event.
    ///
    /// For Binance depth updates this is the `u` (final update ID) field.
    pub sequence: Option<u64>,
    /// The first update ID carried by the event when the exchange provides a
    /// range. For Binance depth updates this is the `U` field.
    pub first_sequence: Option<u64>,
    /// The final update ID of the event that preceded this one when the
    /// exchange provides it. For Binance futures depth updates this is the
    /// `pu` field.
    pub previous_sequence: Option<u64>,
    pub exchange_timestamp: Option<TimestampMilliseconds>,
    /// Base64 encoded raw frame from the exchange.
    pub payload: String,
}

impl ParsedMessage {
    /// Returns the inclusive sequence range covered by this message.
    ///
    /// A message without range metadata covers only its final sequence.
    pub fn sequence_range(&self) -> Option<(u64, u64)> {
        self.sequence
            .map(|last| (self.first_sequence.unwrap_or(last), last))
    }
}

/// A market data event published by the ingester and consumed by the
/// aggregator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketEvent {
    pub exchange: CexId,
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
    pub exchange: CexId,
    pub stream: String,
    pub previous_sequence: u64,
    pub observed_sequence: u64,
    pub missing_from: u64,
    pub missing_to: u64,
    pub detected_at: TimestampMilliseconds,
}
