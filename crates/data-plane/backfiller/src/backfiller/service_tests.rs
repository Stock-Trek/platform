use super::test_support_tests::{
    ControlledStore, OnceFetcher, OneGapSource, RecordingPublisher, SavePolicy, TestFixtures,
};
use crate::{backfiller::BackfillService, gap_retry_store::InMemoryGapRetryStore};
use parking_lot::Mutex;
use platform_market_data::GapMarker;
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn acknowledges_after_processing_a_covered_gap() {
    let acked = Arc::new(Mutex::new(Vec::new()));
    let service = BackfillService::new(
        OneGapSource {
            gap: Some(GapMarker::gap(2, 3)),
            acked: acked.clone(),
        },
        OnceFetcher {
            events: Mutex::new(vec![TestFixtures::event(2), TestFixtures::event(3)]),
        },
        RecordingPublisher::default(),
        TestFixtures::options(),
    );

    let error = service.run().await.unwrap_err();
    assert!(error.to_string().contains("before shutdown"));

    assert_eq!(acked.lock().len(), 1);
}

#[tokio::test]
async fn closes_a_depth_gap_with_a_snapshot_after_the_missing_range() {
    let acked = Arc::new(Mutex::new(Vec::new()));
    let publisher = Arc::new(RecordingPublisher::default());
    let store = Arc::new(InMemoryGapRetryStore::default());
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
            events: Mutex::new(vec![TestFixtures::event(100)]),
        },
        publisher.clone(),
        TestFixtures::options(),
    )
    .with_retry_store_arc(Arc::new(ControlledStore::new(
        store.clone(),
        SavePolicy::Allow,
    )));

    let error = service.run().await.unwrap_err();
    assert!(error.to_string().contains("before shutdown"));

    assert_eq!(*publisher.sequences.lock(), vec![100]);
    assert_eq!(acked.lock().len(), 1);
    assert!(store.snapshot().await.is_empty());
}

#[tokio::test]
async fn closes_a_book_ticker_gap_with_the_observed_snapshot_sequence() {
    let acked = Arc::new(Mutex::new(Vec::new()));
    let publisher = Arc::new(RecordingPublisher::default());
    let store = Arc::new(InMemoryGapRetryStore::default());
    let gap = GapMarker {
        stream: "btcusdt@bookTicker".to_string(),
        ..GapMarker::gap(2, 5)
    };
    let service = BackfillService::new(
        OneGapSource {
            gap: Some(gap),
            acked: acked.clone(),
        },
        OnceFetcher {
            events: Mutex::new(vec![TestFixtures::event(6)]),
        },
        publisher.clone(),
        TestFixtures::options(),
    )
    .with_retry_store_arc(Arc::new(ControlledStore::new(
        store.clone(),
        SavePolicy::Allow,
    )));

    let error = service.run().await.unwrap_err();
    assert!(error.to_string().contains("before shutdown"));

    assert_eq!(*publisher.sequences.lock(), vec![6]);
    assert_eq!(acked.lock().len(), 1);
    assert!(store.snapshot().await.is_empty());
}

#[tokio::test]
async fn persists_partial_ranges_instead_of_forgetting_them() {
    let store = Arc::new(InMemoryGapRetryStore::default());
    let mut service = BackfillService::new(
        OneGapSource {
            gap: Some(GapMarker::gap(2, 5)),
            acked: Arc::new(Mutex::new(Vec::new())),
        },
        OnceFetcher {
            events: Mutex::new(vec![TestFixtures::event(2), TestFixtures::event(4)]),
        },
        RecordingPublisher::default(),
        TestFixtures::options(),
    )
    .with_retry_store_arc(Arc::new(ControlledStore::new(
        store.clone(),
        SavePolicy::Allow,
    )));

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
