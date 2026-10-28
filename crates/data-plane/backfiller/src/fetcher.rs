use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::Value;

/// One exchange record reconstructed from a REST API.
#[derive(Debug, Clone)]
pub struct BackfilledEvent {
    pub sequence: u64,
    pub exchange_timestamp: TimestampMilliseconds,
    /// The exchange-specific `data` object, without the stream envelope.
    pub data: Value,
}

/// Fetches the exchange records covered by a gap marker.
#[async_trait]
pub trait MissingEventFetcher: Send + Sync {
    async fn fetch(&self, gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>>;
}
