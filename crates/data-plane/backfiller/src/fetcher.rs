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

    #[allow(dead_code)]
    pub fn with_fetcher<F>(mut self, exchange: CexId, fetcher: F) -> Self
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

    #[allow(dead_code)]
    pub fn exchanges(&self) -> impl Iterator<Item = CexId> + '_ {
        self.fetchers.keys().copied()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.fetchers.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.fetchers.is_empty()
    }
}

#[async_trait]
impl MissingEventFetcher for FetcherRegistry {
    fn supports(&self, exchange: CexId) -> bool {
        self.contains(exchange)
    }

    async fn fetch(&self, gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
        self.fetcher(gap.exchange)
            .ok_or_else(|| unsupported_exchange(gap.exchange))?
            .fetch(gap)
            .await
    }

    async fn fetch_with_attempt(
        &self,
        gap: &GapMarker,
        attempt: u32,
    ) -> BpResult<Vec<BackfilledEvent>> {
        self.fetcher(gap.exchange)
            .ok_or_else(|| unsupported_exchange(gap.exchange))?
            .fetch_with_attempt(gap, attempt)
            .await
    }
}

fn unsupported_exchange(exchange: CexId) -> BpError {
    BpError::Exchange(ExchangeError::UnsupportedExchange {
        exchange: exchange.to_string(),
    })
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
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct StubFetcher {
        event: BackfilledEvent,
        attempts: Arc<Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl MissingEventFetcher for StubFetcher {
        async fn fetch(&self, _gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
            Ok(vec![self.event.clone()])
        }

        async fn fetch_with_attempt(
            &self,
            _gap: &GapMarker,
            attempt: u32,
        ) -> BpResult<Vec<BackfilledEvent>> {
            self.attempts.lock().expect("attempts lock").push(attempt);
            Ok(vec![self.event.clone()])
        }
    }

    fn gap() -> GapMarker {
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 1,
            observed_sequence: 3,
            missing_from: 2,
            missing_to: 2,
            detected_at: TimestampMilliseconds(1),
        }
    }

    fn event(sequence: u64) -> BackfilledEvent {
        BackfilledEvent {
            sequence,
            exchange_timestamp: TimestampMilliseconds(sequence as i64),
            data: serde_json::json!({ "t": sequence }),
        }
    }

    #[tokio::test]
    async fn dispatches_to_the_fetcher_registered_for_the_gap_exchange() {
        let registry = FetcherRegistry::new()
            .with_fetcher(
                CexId::Binance,
                StubFetcher {
                    event: event(1),
                    attempts: Arc::new(Mutex::new(Vec::new())),
                },
            )
            .with_fetcher(
                CexId::Binance,
                StubFetcher {
                    event: event(2),
                    attempts: Arc::new(Mutex::new(Vec::new())),
                },
            );

        let events = registry.fetch(&gap()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 2);
        assert!(registry.supports(CexId::Binance));
    }

    #[tokio::test]
    async fn forwards_retry_attempts_to_the_selected_fetcher() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let registry = FetcherRegistry::new().with_fetcher(
            CexId::Binance,
            StubFetcher {
                event: event(2),
                attempts: Arc::clone(&attempts),
            },
        );

        registry.fetch_with_attempt(&gap(), 3).await.unwrap();

        assert_eq!(*attempts.lock().unwrap(), vec![3]);
    }

    #[tokio::test]
    async fn rejects_gaps_without_a_registered_fetcher() {
        let registry = FetcherRegistry::new();

        assert!(!registry.supports(CexId::Binance));
        assert!(registry.fetch(&gap()).await.is_err());
    }
}
