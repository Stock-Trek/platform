use super::*;
use platform_market_data::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

impl InMemoryGapRetryStore {
    pub(crate) async fn snapshot(&self) -> Vec<PendingGap> {
        self.pending.lock().await.values().cloned().collect()
    }
}

#[tokio::test]
async fn stores_and_removes_pending_ranges() {
    let store = InMemoryGapRetryStore::default();
    let pending = PendingGap::from_gap(&GapMarker::gap(10, 12), TimestampMilliseconds(2_000));

    store.save(&pending).await.unwrap();
    assert_eq!(store.load().await.unwrap(), vec![pending.clone()]);

    store.remove(&pending.key()).await.unwrap();
    assert!(store.load().await.unwrap().is_empty());
}

#[test]
fn retry_delay_backs_off_and_is_capped() {
    assert_eq!(PendingGap::retry_delay(0), RETRY_DELAY);
    assert_eq!(PendingGap::retry_delay(2), RETRY_DELAY * 4);
    assert_eq!(PendingGap::retry_delay(100), MAX_RETRY_DELAY);
}

#[test]
fn pending_ranges_reconstruct_gap_markers() {
    let pending = PendingGap::from_range(
        &GapMarker::gap(10, 12),
        11,
        12,
        3,
        TimestampMilliseconds(5_000),
        Some("empty".to_string()),
    );

    assert_eq!(
        pending.gap_marker(TimestampMilliseconds(6_000)),
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 10,
            observed_sequence: 13,
            missing_from: 11,
            missing_to: 12,
            detected_at: TimestampMilliseconds(6_000),
        }
    );
}
