use crate::{
    fetcher::{BackfilledEvent, MissingEventFetcher},
    gap_marker_source::GapMarkerSource,
    retry::{GapRetryStore, InMemoryGapRetryStore, PendingGap, retry_at},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    envelope::EnvelopeCodec,
    event::{GapMarker, MarketEvent},
    subject::MarketEventSubject,
};
use platform_metrics::descriptions::{
    BACKFILL_OUTCOME_COVERED, BACKFILL_OUTCOME_EMPTY, BACKFILL_OUTCOME_ERROR,
    BACKFILL_OUTCOME_PARTIAL,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::EventPublisher;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use stock_trek_types::cex::cex_id::CexId;
use tokio::time::{Duration, Instant, sleep_until};
use tracing::{info, warn};

const FALLBACK_WINDOW_STEP: u64 = 1_000;

#[derive(Debug, Clone)]
pub struct BackfillOptions {
    pub event_subject_prefix: String,
}

impl BackfillOptions {
    pub fn subject_for(&self, exchange: CexId, stream: &str) -> String {
        MarketEventSubject::new(&self.event_subject_prefix, exchange).event(stream)
    }
}

/// Turns gap markers into reconstructed market events on the ingester's event
/// subjects.
pub struct BackfillService<S, F, P> {
    source: S,
    fetcher: F,
    publisher: P,
    options: BackfillOptions,
    retries: Arc<dyn GapRetryStore>,
}

impl<S, F, P> BackfillService<S, F, P>
where
    S: GapMarkerSource,
    F: MissingEventFetcher,
    P: EventPublisher,
{
    pub fn new(source: S, fetcher: F, publisher: P, options: BackfillOptions) -> Self {
        Self {
            source,
            fetcher,
            publisher,
            options,
            retries: Arc::new(InMemoryGapRetryStore::new()),
        }
    }

    pub fn with_retry_store_arc(mut self, retries: Arc<dyn GapRetryStore>) -> Self {
        self.retries = retries;
        self
    }

    pub async fn run(mut self) -> BpResult<()> {
        let mut pending = self.load_pending().await?;
        let mut source_ended = false;

        loop {
            let now = TimestampMilliseconds::now();
            if let Some(key) = next_due_key(&pending, now) {
                if let Some(gap) = pending.remove(&key) {
                    let retry = gap.clone();
                    if let Err(error) = self.attempt(gap, &mut pending).await {
                        self.requeue_failed_retry(&error, retry, &mut pending);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                continue;
            }

            if source_ended {
                info!("gap marker source ended; retrying pending backfill ranges");
                let keys: Vec<_> = pending.keys().cloned().collect();
                for key in keys {
                    if let Some(gap) = pending.remove(&key) {
                        let retry = gap.clone();
                        if let Err(error) = self.attempt(gap, &mut pending).await {
                            self.requeue_failed_retry(&error, retry, &mut pending);
                        }
                    }
                }
                return Ok(());
            }

            let next_attempt = next_attempt_at(&pending);
            let next_wake = next_attempt.map(instant_at);
            tokio::select! {
                gap = self.source.next_gap() => {
                    match gap? {
                        Some(gap) => self.accept(gap, &mut pending).await,
                        None => {
                            source_ended = true;
                        }
                    }
                }
                _ = sleep_until_optional(next_wake), if next_wake.is_some() => {}
                signal = platform_runtime::Runtime::shutdown_signal() => {
                    signal?;
                    return Ok(());
                }
            }
        }
    }

    fn requeue_failed_retry(
        &self,
        error: &BpError,
        gap: PendingGap,
        pending: &mut BTreeMap<String, PendingGap>,
    ) {
        MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_ERROR);
        warn!(%error, "failed to retry pending backfill range");
        pending.entry(gap.key()).or_insert(gap);
    }

    async fn load_pending(&self) -> BpResult<BTreeMap<String, PendingGap>> {
        let mut pending = BTreeMap::new();
        for gap in self.retries.load().await? {
            pending.insert(gap.key(), gap);
        }
        Ok(pending)
    }

    async fn accept(&mut self, gap: GapMarker, pending: &mut BTreeMap<String, PendingGap>) {
        if !self.fetcher.supports(gap.exchange) {
            let _ = self.source.ack(&gap).await;
            return;
        }

        let now = TimestampMilliseconds::now();
        let initial = PendingGap::from_gap(&gap, now);
        let key = initial.key();
        if let Err(error) = self.retries.save(&initial).await {
            warn!(%error, "failed to persist backfill range; leaving marker unacknowledged");
            return;
        }
        pending.insert(key, initial.clone());

        if let Err(error) = self.attempt(initial, pending).await {
            warn!(%error, "failed to process backfill range");
        }
        if let Err(error) = self.source.ack(&gap).await {
            warn!(%error, "failed to acknowledge processed gap marker");
        }
    }

    async fn attempt(
        &self,
        pending: PendingGap,
        all_pending: &mut BTreeMap<String, PendingGap>,
    ) -> BpResult<()> {
        let now = TimestampMilliseconds::now();
        let requested = pending.gap_marker(now);
        let fetch_gap = widened_gap(&requested, pending.attempts);
        let result = self
            .fetcher
            .fetch_with_attempt(&fetch_gap, pending.attempts)
            .await;

        let (events, error) = match result {
            Ok(events) => (events, None),
            Err(error) => (Vec::new(), Some(error.to_string())),
        };
        let events = self.publish_filtered(&requested, events).await;
        let events = match events {
            Ok(events) => events,
            Err(error) => {
                MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_ERROR);
                warn!(
                    exchange = %pending.exchange,
                    stream = %pending.stream,
                    %error,
                    "failed to publish backfilled events"
                );
                let ranges = vec![(pending.missing_from, pending.missing_to)];
                return self
                    .schedule(
                        requested,
                        pending,
                        all_pending,
                        ranges,
                        Some(error.to_string()),
                    )
                    .await;
            }
        };

        let expected = pending.missing_to.saturating_sub(pending.missing_from) + 1;
        let covered = events.len() as u64;
        let unresolved = missing_ranges(
            pending.missing_from,
            pending.missing_to,
            events.iter().map(|event| event.sequence),
        );

        if expected > 0 {
            MarketDataMetrics::record_backfill_gap_coverage(expected, covered);
        }
        if let Some(error) = &error {
            MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_ERROR);
            warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                %error,
                "failed to fetch gap from exchange"
            );
        } else if events.is_empty() {
            MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_EMPTY);
            warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                "exchange returned no events for gap"
            );
        } else if !unresolved.is_empty() {
            MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_PARTIAL);
            warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                expected,
                published = covered,
                "backfill did not cover the whole gap"
            );
        } else {
            MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_COVERED);
            info!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                published = covered,
                "backfilled gap"
            );
        }

        if unresolved.is_empty() && error.is_none() {
            self.remove(&pending, all_pending).await
        } else {
            self.schedule(requested, pending, all_pending, unresolved, error)
                .await
        }
    }

    async fn publish_filtered(
        &self,
        gap: &GapMarker,
        events: Vec<BackfilledEvent>,
    ) -> BpResult<Vec<BackfilledEvent>> {
        let mut seen = HashSet::new();
        let mut events: Vec<_> = events
            .into_iter()
            .filter(|event| {
                event.sequence >= gap.missing_from
                    && event.sequence <= gap.missing_to
                    && seen.insert(event.sequence)
            })
            .collect();
        events.sort_by_key(|event| event.sequence);

        for event in &events {
            self.publish(gap, event.clone()).await?;
        }
        Ok(events)
    }

    async fn remove(
        &self,
        pending: &PendingGap,
        all_pending: &mut BTreeMap<String, PendingGap>,
    ) -> BpResult<()> {
        let key = pending.key();
        self.retries.remove(&key).await?;
        all_pending.remove(&key);
        Ok(())
    }

    async fn schedule(
        &self,
        requested: GapMarker,
        pending: PendingGap,
        all_pending: &mut BTreeMap<String, PendingGap>,
        ranges: Vec<(u64, u64)>,
        last_error: Option<String>,
    ) -> BpResult<()> {
        let now = TimestampMilliseconds::now();
        let attempts = pending.attempts.saturating_add(1);
        let next_attempt_at = retry_at(now, attempts);

        let parent_key = pending.key();
        let mut child_keys = HashSet::new();
        for (missing_from, missing_to) in ranges {
            let child = PendingGap::from_range(
                &requested,
                missing_from,
                missing_to,
                attempts,
                next_attempt_at,
                last_error.clone(),
            );
            let key = child.key();
            self.retries.save(&child).await?;
            child_keys.insert(key.clone());
            all_pending.insert(key, child);
        }
        if !child_keys.contains(&parent_key) {
            self.retries.remove(&parent_key).await?;
            all_pending.remove(&parent_key);
        }
        Ok(())
    }

    async fn publish(&self, gap: &GapMarker, event: BackfilledEvent) -> BpResult<()> {
        let frame = json!({
            "stream": gap.stream,
            "data": event.data,
        });
        let payload = BASE64.encode(serde_json::to_vec(&frame)?);
        let market_event = MarketEvent {
            exchange: gap.exchange,
            stream: gap.stream.clone(),
            sequence: Some(event.sequence),
            exchange_timestamp: Some(event.exchange_timestamp),
            received_at: TimestampMilliseconds::now(),
            payload,
        };
        self.publisher
            .publish(
                &self.options.subject_for(gap.exchange, &gap.stream),
                &market_event.encode()?,
            )
            .await
    }
}

pub fn missing_ranges(
    missing_from: u64,
    missing_to: u64,
    sequences: impl IntoIterator<Item = u64>,
) -> Vec<(u64, u64)> {
    if missing_from > missing_to {
        return Vec::new();
    }
    let mut sequences: Vec<u64> = sequences
        .into_iter()
        .filter(|sequence| *sequence >= missing_from && *sequence <= missing_to)
        .collect();
    sequences.sort_unstable();
    sequences.dedup();

    let mut ranges = Vec::new();
    let mut cursor = missing_from;
    for sequence in sequences {
        if sequence > cursor {
            ranges.push((cursor, sequence - 1));
        }
        if sequence == u64::MAX {
            return ranges;
        }
        cursor = sequence + 1;
        if cursor > missing_to {
            return ranges;
        }
    }
    if cursor <= missing_to {
        ranges.push((cursor, missing_to));
    }
    ranges
}

fn widened_gap(gap: &GapMarker, attempts: u32) -> GapMarker {
    if attempts == 0 {
        return gap.clone();
    }
    let padding = FALLBACK_WINDOW_STEP.saturating_mul(u64::from(attempts.min(32)));
    let mut widened = gap.clone();
    widened.previous_sequence = gap.missing_from.saturating_sub(padding).saturating_sub(1);
    widened.missing_from = gap.missing_from.saturating_sub(padding);
    widened.missing_to = gap.missing_to.saturating_add(padding);
    widened.observed_sequence = gap.missing_to.saturating_add(padding).saturating_add(1);
    widened
}

fn next_due_key(
    pending: &BTreeMap<String, PendingGap>,
    now: TimestampMilliseconds,
) -> Option<String> {
    pending
        .iter()
        .find(|(_, gap)| gap.is_due(now))
        .map(|(key, _)| key.clone())
}

fn next_attempt_at(pending: &BTreeMap<String, PendingGap>) -> Option<TimestampMilliseconds> {
    pending.values().map(|gap| gap.next_attempt_at).min()
}

fn instant_at(timestamp: TimestampMilliseconds) -> Instant {
    let now = TimestampMilliseconds::now().0;
    let delay = timestamp.0.saturating_sub(now).max(0) as u64;
    Instant::now() + Duration::from_millis(delay)
}

async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{gap_marker_source::GapMarkerSource, retry::GapRetryStore};
    use async_trait::async_trait;
    use platform_errors::{BpError, BpResult};
    use platform_market_data::event::GapMarker;
    use std::sync::Mutex;

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

    fn gap(from: u64, to: u64) -> GapMarker {
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: from.saturating_sub(1),
            observed_sequence: to.saturating_add(1),
            missing_from: from,
            missing_to: to,
            detected_at: TimestampMilliseconds(1),
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
                gap: Some(gap(2, 3)),
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
    async fn persists_partial_ranges_instead_of_forgetting_them() {
        let store = Arc::new(InMemoryGapRetryStore::new());
        let mut service = BackfillService::new(
            OneGapSource {
                gap: Some(gap(2, 5)),
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
        service.accept(gap(2, 5), &mut pending).await;

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
        let pending = PendingGap::from_range(&gap(2, 3), 2, 3, 0, TimestampMilliseconds(0), None);
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
        assert_eq!(missing_ranges(10, 15, [10, 11, 12, 13, 14, 15]), vec![]);
        assert_eq!(
            missing_ranges(10, 15, [10, 12, 15]),
            vec![(11, 11), (13, 14)]
        );
        assert_eq!(missing_ranges(10, 15, []), vec![(10, 15)]);
        assert_eq!(missing_ranges(10, 10, [10]), vec![]);
    }

    #[test]
    fn retry_attempts_widen_the_rest_window() {
        let widened = widened_gap(&gap(10_000, 20_000), 2);

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

    #[allow(dead_code)]
    fn _assert_error_is_send_sync() {
        fn assert<T: Send + Sync>() {}
        assert::<BpError>();
    }
}
