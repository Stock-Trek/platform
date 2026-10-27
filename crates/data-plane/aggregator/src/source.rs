use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::event::MarketEvent;

/// Source of ingester market events.
#[async_trait]
pub trait MarketEventSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>>;
}
