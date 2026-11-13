use super::test_support_tests::{
    ControlledStore, OnceFetcher, OneGapSource, RecordingPublisher, SavePolicy, StreamRangeFetcher,
    TestFixtures,
};
use crate::{
    backfiller::BackfillService,
    gap_retry_store::{GapRetryStore, InMemoryGapRetryStore, PendingGap},
};
use parking_lot::Mutex;
use platform_market_data::GapMarker;
use platform_types::TimestampMilliseconds;
use std::{collections::BTreeMap, sync::Arc};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn retries_pending_ranges_after_the_source_ends() {
    let store = Arc::new(InMemoryGapRetryStore::default());
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
            events: Mutex::new(vec![TestFixtures::event(2), TestFixtures::event(3)]),
        },
        RecordingPublisher::default(),
        TestFixtures::options(),
    )
    .with_retry_store_arc(Arc::new(ControlledStore::new(
        store.clone(),
        SavePolicy::Allow,
    )));

    let error = service.run().await.unwrap_err();
    assert!(error.to_string().contains("before shutdown"));

    assert!(store.snapshot().await.is_empty());
}

#[tokio::test]
async fn delays_a_gap_whose_retry_cannot_be_persisted() {
    let store = Arc::new(InMemoryGapRetryStore::default());
    let mut service = BackfillService::new(
        OneGapSource {
            gap: None,
            acked: Arc::new(Mutex::new(Vec::new())),
        },
        OnceFetcher {
            events: Mutex::new(Vec::new()),
        },
        RecordingPublisher::default(),
        TestFixtures::options(),
    )
    .with_retry_store_arc(Arc::new(ControlledStore::new(
        store,
        SavePolicy::AfterFirstSave,
    )));

    let mut pending = BTreeMap::new();
    service.accept(GapMarker::gap(2, 5), &mut pending).await;

    assert_eq!(pending.len(), 1);
    let gap = pending.values().next().unwrap();
    assert_eq!((gap.missing_from, gap.missing_to), (2, 5));
    assert_eq!(gap.attempts, 1);
    assert!(!gap.is_due(TimestampMilliseconds::now()));
}

#[tokio::test]
async fn failed_retry_does_not_block_new_gap_markers() {
    let store = Arc::new(InMemoryGapRetryStore::default());
    let failed = PendingGap::from_range(
        &GapMarker {
            stream: "empty".to_string(),
            ..GapMarker::gap(2, 3)
        },
        2,
        3,
        0,
        TimestampMilliseconds(0),
        None,
    );
    store.save(&failed).await.unwrap();

    let acked = Arc::new(Mutex::new(Vec::new()));
    let publisher = Arc::new(RecordingPublisher::default());
    let service = BackfillService::new(
        OneGapSource {
            gap: Some(GapMarker::gap(10, 11)),
            acked: acked.clone(),
        },
        StreamRangeFetcher {
            empty_stream: "empty".to_string(),
        },
        publisher.clone(),
        TestFixtures::options(),
    )
    .with_retry_store_arc(Arc::new(ControlledStore::new(
        store,
        SavePolicy::ForStream("empty"),
    )));

    let error = timeout(Duration::from_secs(2), service.run())
        .await
        .expect("service stalled after a failed retry")
        .unwrap_err();
    assert!(error.to_string().contains("before shutdown"));

    assert_eq!(*publisher.sequences.lock(), vec![10, 11]);
    let acked = acked.lock();
    assert_eq!(acked.len(), 1);
    assert_eq!(acked[0].missing_from, 10);
}
