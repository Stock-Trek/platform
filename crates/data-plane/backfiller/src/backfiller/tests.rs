use super::*;
use crate::{
    fetcher::{BackfilledEvent, MissingEventFetcher},
    gap_marker_source::GapMarkerSource,
    retry::{GapRetryStore, InMemoryGapRetryStore, PendingGap},
};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::event::{GapMarker, MarketEvent};
use platform_nats::EventPublisher;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

struct OneGapSource {
    gap: Option<GapMarker>,
    acked: Arc<Mutex<Vec<GapMarker>>>,
}

#[async_trait]
impl GapMarkerSource for OneGapSource {
    async fn next_gap(&mut self) -> BpResult<Option<GapMarker>> {
        Ok(self.gap.take())
    }

    async fn ack(&mut self, gap: &GapMarker) -> BpResult<()> {
        self.acked.lock().expect("acked lock").push(gap.clone());
        Ok(())
    }
}

struct OnceFetcher {
    events: Mutex<Vec<BackfilledEvent>>,
}

#[async_trait]
impl MissingEventFetcher for OnceFetcher {
    async fn fetch(&self, _gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
        Ok(std::mem::take(
            &mut *self.events.lock().expect("events lock"),
        ))
    }
}

#[derive(Default)]
struct RecordingPublisher {
    sequences: Mutex<Vec<u64>>,
}

#[async_trait]
impl EventPublisher for RecordingPublisher {
    async fn publish(&self, _subject: &str, payload: &[u8]) -> BpResult<()> {
        let event: MarketEvent = serde_json::from_slice(payload)?;
        self.sequences
            .lock()
            .expect("sequences lock")
            .push(event.sequence.unwrap_or_default());
        Ok(())
    }
}

fn event(sequence: u64) -> BackfilledEvent {
    BackfilledEvent {
        sequence,
        exchange_timestamp: TimestampMilliseconds(sequence as i64),
        data: json!({ "t": sequence }),
    }
}

fn options() -> BackfillOptions {
    BackfillOptions {
        event_subject_prefix: "market.events".to_string(),
    }
}

#[tokio::test]
async fn acknowledges_after_processing_a_covered_gap() {
    let acked = Arc::new(Mutex::new(Vec::new()));
    let service = BackfillService::new(
        OneGapSource {
            gap: Some(GapMarker::gap(2, 3)),
            acked: acked.clone(),
        },
        OnceFetcher {
            events: Mutex::new(vec![event(2), event(3)]),
        },
        RecordingPublisher::default(),
        options(),
    );

    service.run().await.unwrap();

    assert_eq!(acked.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn closes_a_depth_gap_with_a_snapshot_after_the_missing_range() {
    let acked = Arc::new(Mutex::new(Vec::new()));
    let publisher = Arc::new(RecordingPublisher::default());
    let store = Arc::new(InMemoryGapRetryStore::new());
    let gap = GapMarker {
        stream: "btcusdt@depth".to_string(),
        ..GapMarker::gap(2, 5)
    };
    let service = BackfillService::new(
        OneGapSource {
            gap: Some(gap),
            acked: acked.clone(),
        },
        OnceFetcher {
            events: Mutex::new(vec![event(100)]),
        },
        publisher.clone(),
        options(),
    )
    .with_retry_store_arc(Arc::new(SharedStore(store.clone())));

    service.run().await.unwrap();

    assert_eq!(*publisher.sequences.lock().unwrap(), vec![100]);
    assert_eq!(acked.lock().unwrap().len(), 1);
    assert!(store.snapshot().await.is_empty());
}

#[tokio::test]
async fn persists_partial_ranges_instead_of_forgetting_them() {
    let store = Arc::new(InMemoryGapRetryStore::new());
    let mut service = BackfillService::new(
        OneGapSource {
            gap: Some(GapMarker::gap(2, 5)),
            acked: Arc::new(Mutex::new(Vec::new())),
        },
        OnceFetcher {
            events: Mutex::new(vec![event(2), event(4)]),
        },
        RecordingPublisher::default(),
        options(),
    )
    .with_retry_store_arc(Arc::new(SharedStore(store.clone())));

    let pending = service.load_pending().await.unwrap();
    assert!(pending.is_empty());

    let mut pending = BTreeMap::new();
    service.accept(GapMarker::gap(2, 5), &mut pending).await;

    let mut gaps = store.snapshot().await;
    gaps.sort_by_key(|gap| (gap.missing_from, gap.missing_to));
    assert_eq!(
        gaps.iter()
            .map(|gap| (gap.missing_from, gap.missing_to))
            .collect::<Vec<_>>(),
        vec![(3, 3), (5, 5)]
    );
}

#[tokio::test]
async fn retries_pending_ranges_after_the_source_ends() {
    let store = Arc::new(InMemoryGapRetryStore::new());
    let pending = PendingGap::from_range(
        &GapMarker::gap(2, 3),
        2,
        3,
        0,
        TimestampMilliseconds(0),
        None,
    );
    store.save(&pending).await.unwrap();

    let service = BackfillService::new(
        OneGapSource {
            gap: None,
            acked: Arc::new(Mutex::new(Vec::new())),
        },
        OnceFetcher {
            events: Mutex::new(vec![event(2), event(3)]),
        },
        RecordingPublisher::default(),
        options(),
    )
    .with_retry_store_arc(Arc::new(SharedStore(store.clone())));

    service.run().await.unwrap();

    assert!(store.snapshot().await.is_empty());
}

#[test]
fn finds_contiguous_unresolved_ranges() {
    assert_eq!(
        PendingGap::missing_ranges(10, 15, [10, 11, 12, 13, 14, 15]),
        vec![]
    );
    assert_eq!(
        PendingGap::missing_ranges(10, 15, [10, 12, 15]),
        vec![(11, 11), (13, 14)]
    );
    assert_eq!(PendingGap::missing_ranges(10, 15, []), vec![(10, 15)]);
    assert_eq!(PendingGap::missing_ranges(10, 10, [10]), vec![]);
}

#[test]
fn retry_attempts_widen_the_rest_window() {
    let widened = PendingGap::widened_gap(&GapMarker::gap(10_000, 20_000), 2);

    assert_eq!(widened.missing_from, 8_000);
    assert_eq!(widened.missing_to, 22_000);
    assert_eq!(widened.previous_sequence, 7_999);
    assert_eq!(widened.observed_sequence, 22_001);
}

struct SharedStore(Arc<InMemoryGapRetryStore>);

#[async_trait]
impl GapRetryStore for SharedStore {
    async fn load(&self) -> BpResult<Vec<PendingGap>> {
        self.0.load().await
    }

    async fn save(&self, pending: &PendingGap) -> BpResult<()> {
        self.0.save(pending).await
    }

    async fn remove(&self, key: &str) -> BpResult<()> {
        self.0.remove(key).await
    }
}
