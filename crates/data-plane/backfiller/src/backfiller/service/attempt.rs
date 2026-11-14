use super::{BackfillService, outcome::GapOutcome};
use crate::{
    event_fetcher::{BackfilledEvent, MissingEventFetcher},
    gap_retry_store::PendingGap,
    nats_gap_marker_source::GapMarkerSource,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::BuildResult;
use platform_market_data::{EnvelopeCodec, GapMarker, MarketEvent};
use platform_metrics::MarketDataMetrics;
use platform_nats::EventPublisher;
use platform_types::TimestampMilliseconds;
use serde_json::json;
use std::collections::{BTreeMap, HashSet};

impl<S, F, P> BackfillService<S, F, P>
where
    S: GapMarkerSource,
    F: MissingEventFetcher,
    P: EventPublisher,
{
    pub async fn attempt(
        &self,
        pending: PendingGap,
        all_pending: &mut BTreeMap<String, PendingGap>,
    ) -> BuildResult<()> {
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
    ) -> BuildResult<Vec<BackfilledEvent>> {
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
    ) -> BuildResult<()> {
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
    ) -> BuildResult<()> {
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

    async fn publish(&self, gap: &GapMarker, event: BackfilledEvent) -> BuildResult<()> {
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
