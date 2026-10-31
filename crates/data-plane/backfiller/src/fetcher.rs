use crate::event_page_source::EventPageSource;
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_market_data::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use stock_trek_types::cex::cex_id::CexId;

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
    fn supports(&self, _exchange: CexId) -> bool {
        true
    }

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

#[async_trait]
impl<T> MissingEventFetcher for Arc<T>
where
    T: MissingEventFetcher + ?Sized,
{
    fn supports(&self, exchange: CexId) -> bool {
        (**self).supports(exchange)
    }

    async fn fetch(&self, gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
        (**self).fetch(gap).await
    }

    async fn fetch_with_attempt(
        &self,
        gap: &GapMarker,
        attempt: u32,
    ) -> BpResult<Vec<BackfilledEvent>> {
        (**self).fetch_with_attempt(gap, attempt).await
    }
}

#[derive(Default)]
pub struct FetcherRegistry {
    fetchers: HashMap<CexId, Arc<dyn MissingEventFetcher>>,
}

impl FetcherRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, exchange: CexId, fetcher: F) -> &mut Self
    where
        F: MissingEventFetcher + 'static,
    {
        self.insert(exchange, fetcher);
        self
    }

    pub fn insert<F>(&mut self, exchange: CexId, fetcher: F) -> Option<Arc<dyn MissingEventFetcher>>
    where
        F: MissingEventFetcher + 'static,
    {
        self.fetchers.insert(exchange, Arc::new(fetcher))
    }

    pub fn get(&self, exchange: CexId) -> Option<&Arc<dyn MissingEventFetcher>> {
        self.fetchers.get(&exchange)
    }

    pub fn fetcher(&self, exchange: CexId) -> Option<&dyn MissingEventFetcher> {
        self.get(exchange).map(|fetcher| fetcher.as_ref())
    }

    pub fn contains(&self, exchange: CexId) -> bool {
        self.fetchers.contains_key(&exchange)
    }

    fn unsupported_exchange(exchange: CexId) -> BpError {
        BpError::Exchange(ExchangeError::UnsupportedExchange {
            exchange: exchange.to_string(),
        })
    }
}

#[async_trait]
impl MissingEventFetcher for FetcherRegistry {
    fn supports(&self, exchange: CexId) -> bool {
        self.contains(exchange)
    }

    async fn fetch(&self, gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
        self.fetcher(gap.exchange)
            .ok_or_else(|| Self::unsupported_exchange(gap.exchange))?
            .fetch(gap)
            .await
    }

    async fn fetch_with_attempt(
        &self,
        gap: &GapMarker,
        attempt: u32,
    ) -> BpResult<Vec<BackfilledEvent>> {
        self.fetcher(gap.exchange)
            .ok_or_else(|| Self::unsupported_exchange(gap.exchange))?
            .fetch_with_attempt(gap, attempt)
            .await
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

#[cfg(test)]
mod tests;
