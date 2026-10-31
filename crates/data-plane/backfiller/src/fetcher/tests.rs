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

fn event(sequence: u64) -> BackfilledEvent {
    BackfilledEvent {
        sequence,
        exchange_timestamp: TimestampMilliseconds(sequence as i64),
        data: serde_json::json!({ "t": sequence }),
    }
}

#[tokio::test]
async fn dispatches_to_the_fetcher_registered_for_the_gap_exchange() {
    let mut registry = FetcherRegistry::new();
    registry.register(
        CexId::Binance,
        StubFetcher {
            event: event(1),
            attempts: Arc::new(Mutex::new(Vec::new())),
        },
    );
    registry.register(
        CexId::Binance,
        StubFetcher {
            event: event(2),
            attempts: Arc::new(Mutex::new(Vec::new())),
        },
    );

    let events = registry.fetch(&GapMarker::gap(2, 2)).await.unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 2);
    assert!(registry.supports(CexId::Binance));
}

#[tokio::test]
async fn forwards_retry_attempts_to_the_selected_fetcher() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = FetcherRegistry::new();
    registry.register(
        CexId::Binance,
        StubFetcher {
            event: event(2),
            attempts: Arc::clone(&attempts),
        },
    );

    registry
        .fetch_with_attempt(&GapMarker::gap(2, 2), 3)
        .await
        .unwrap();

    assert_eq!(*attempts.lock().unwrap(), vec![3]);
}

#[tokio::test]
async fn rejects_gaps_without_a_registered_fetcher() {
    let registry = FetcherRegistry::new();

    assert!(!registry.supports(CexId::Binance));
    assert!(registry.fetch(&GapMarker::gap(2, 2)).await.is_err());
}
