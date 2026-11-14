use crate::event_fetcher::BackfilledEvent;
use async_trait::async_trait;
use platform_errors::PlatformResult;

/// A page of exchange records ordered by sequence id.
#[async_trait]
pub trait EventPageSource: Send + Sync {
    async fn fetch_page(
        &self,
        symbol: &str,
        from_sequence: u64,
        limit: u32,
    ) -> PlatformResult<Vec<BackfilledEvent>>;
}
