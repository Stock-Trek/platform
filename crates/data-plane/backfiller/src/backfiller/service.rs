use crate::{
    fetcher::{BackfilledEvent, MissingEventFetcher},
    gap_marker_source::GapMarkerSource,
    retry::{GapRetryStore, InMemoryGapRetryStore, PendingGap},
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
use platform_runtime::Runtime;
use platform_types::new_types::TimestampMilliseconds;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use stock_trek_types::cex::cex_id::CexId;
use tokio::time::Duration;
use tracing::{info, warn};

#[derive(Clone, Copy)]
enum GapOutcome<'a> {
    Error {
        error: &'a str,
        message: &'static str,
    },
    Empty,
    Partial {
        expected: u64,
        published: u64,
    },
    Covered {
        published: u64,
    },
}

impl GapOutcome<'_> {
    fn record(self, pending: &PendingGap) {
        MarketDataMetrics::record_backfill_gap_outcome(self.metric());
        match self {
            Self::Error { error, message } => warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                %error,
                "{message}"
            ),
            Self::Empty => warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                "exchange returned no events for gap"
            ),
            Self::Partial {
                expected,
                published,
            } => warn!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                expected,
                published,
                "backfill did not cover the whole gap"
            ),
            Self::Covered { published } => info!(
                exchange = %pending.exchange,
                stream = %pending.stream,
                missing_from = pending.missing_from,
                missing_to = pending.missing_to,
                published,
                "backfilled gap"
            ),
        }
    }

    fn metric(self) -> &'static str {
        match self {
            Self::Error { .. } => BACKFILL_OUTCOME_ERROR,
            Self::Empty => BACKFILL_OUTCOME_EMPTY,
            Self::Partial { .. } => BACKFILL_OUTCOME_PARTIAL,
            Self::Covered { .. } => BACKFILL_OUTCOME_COVERED,
        }
    }
}

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

    pub async fn run(self) -> BpResult<()> {
        let pending = self.load_pending().await?;
        Runtime::run_until_shutdown(self.process_gaps(pending)).await
    }

    async fn process_gaps(mut self, mut pending: BTreeMap<String, PendingGap>) -> BpResult<()> {
        let mut source_ended = false;

        loop {
            let now = TimestampMilliseconds::now();
            if let Some(key) = PendingGap::next_due_key(&pending, now) {
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

            let next_attempt = PendingGap::next_attempt_at(&pending);
            let next_wake = next_attempt.map(PendingGap::instant_at);
            tokio::select! {
                gap = self.source.next_gap() => {
                    match gap? {
                        Some(gap) => self.accept(gap, &mut pending).await,
                        None => {
                            source_ended = true;
                        }
                    }
                }
                _ = PendingGap::sleep_until_optional(next_wake), if next_wake.is_some() => {}
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

    pub(super) async fn load_pending(&self) -> BpResult<BTreeMap<String, PendingGap>> {
        let mut pending = BTreeMap::new();
        for gap in self.retries.load().await? {
            pending.insert(gap.key(), gap);
        }
        Ok(pending)
    }

    pub(super) async fn accept(
        &mut self,
        gap: GapMarker,
        pending: &mut BTreeMap<String, PendingGap>,
    ) {
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
        let fetch_gap = PendingGap::widened_gap(&requested, pending.attempts);
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
                let error = error.to_string();
                GapOutcome::Error {
                    error: &error,
                    message: "failed to publish backfilled events",
                }
                .record(&pending);
                let ranges = vec![(pending.missing_from, pending.missing_to)];
                return self
                    .schedule(requested, pending, all_pending, ranges, Some(error))
                    .await;
            }
        };

        let expected = pending.missing_to.saturating_sub(pending.missing_from) + 1;
        let covered = events.len() as u64;
        let snapshot_supersedes_gap = events
            .iter()
            .any(|event| event.sequence > pending.missing_to);
        let unresolved = if snapshot_supersedes_gap {
            Vec::new()
        } else {
            PendingGap::missing_ranges(
                pending.missing_from,
                pending.missing_to,
                events.iter().map(|event| event.sequence),
            )
        };

        if expected > 0 {
            MarketDataMetrics::record_backfill_gap_coverage(expected, covered);
        }
        let outcome = if let Some(error) = &error {
            GapOutcome::Error {
                error,
                message: "failed to fetch gap from exchange",
            }
        } else if events.is_empty() {
            GapOutcome::Empty
        } else if !unresolved.is_empty() {
            GapOutcome::Partial {
                expected,
                published: covered,
            }
        } else {
            GapOutcome::Covered { published: covered }
        };
        outcome.record(&pending);

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
                let in_range =
                    event.sequence >= gap.missing_from && event.sequence <= gap.missing_to;
                // Snapshot-based streams, such as Binance depth, cannot replay
                // historical update IDs. A snapshot fetched after the gap is
                // newer than `missing_to` and supersedes the missing range.
                let supersedes_gap = event.sequence > gap.missing_to;
                (in_range || supersedes_gap) && seen.insert(event.sequence)
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
        let next_attempt_at = PendingGap::retry_at(now, attempts);

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
