use crate::event_page_source::EventPageSource;
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_market_data::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::Value;

const PAGE_LIMIT: u64 = 1_000;

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

    /// Fetches a gap with knowledge of how many durable retries have already
    /// failed. Implementations may use this to widen their REST window.
    async fn fetch_with_attempt(
        &self,
        gap: &GapMarker,
        _attempt: u32,
    ) -> BpResult<Vec<BackfilledEvent>> {
        self.fetch(gap).await
    }
}

/// Shared pagination structure for exchange-specific REST fetchers.
pub struct PaginatedFetcher<S> {
    source: S,
}

impl<S> PaginatedFetcher<S>
where
    S: EventPageSource,
{
    pub fn new(source: S) -> Self {
        Self { source }
    }

    pub async fn fetch_range(
        &self,
        symbol: &str,
        missing_from: u64,
        missing_to: u64,
    ) -> BpResult<Vec<BackfilledEvent>> {
        if missing_from > missing_to {
            return Err(BpError::Exchange(ExchangeError::BadResponse(format!(
                "gap on {symbol} has missing_from after missing_to"
            ))));
        }

        let mut next_sequence = missing_from;
        let mut events = Vec::new();
        while next_sequence <= missing_to {
            let remaining = missing_to - next_sequence + 1;
            let limit = remaining.min(PAGE_LIMIT) as u32;
            let page = self.source.fetch_page(symbol, next_sequence, limit).await?;
            if page.is_empty() {
                break;
            }

            let mut highest_sequence = next_sequence.saturating_sub(1);
            for event in page {
                highest_sequence = highest_sequence.max(event.sequence);
                if event.sequence >= next_sequence && event.sequence <= missing_to {
                    events.push(event);
                }
            }
            if highest_sequence < next_sequence {
                return Err(BpError::Exchange(ExchangeError::BadResponse(format!(
                    "{symbol} returned events before the requested id {next_sequence}"
                ))));
            }
            if highest_sequence == u64::MAX {
                break;
            }
            next_sequence = highest_sequence + 1;
        }
        Ok(events)
    }
}
