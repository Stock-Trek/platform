use crate::{
    event_fetcher::MissingEventFetcher,
    gap_retry_store::{GapRetryStore, InMemoryGapRetryStore, PendingGap},
    nats_gap_marker_source::GapMarkerSource,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_market_data::{GapMarker, MarketEventSubject};
use platform_metrics::BACKFILL_OUTCOME_ERROR;
use platform_metrics::MarketDataMetrics;
use platform_nats::EventPublisher;
use platform_runtime::Runtime;
use platform_types::TimestampMilliseconds;
use std::{collections::BTreeMap, sync::Arc};
use stock_trek_types::cex::cex_id::CexId;
use tracing::{info, warn};

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
    pub source: S,
    pub fetcher: F,
    pub publisher: P,
    pub options: BackfillOptions,
    pub retries: Arc<dyn GapRetryStore>,
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
            retries: Arc::new(InMemoryGapRetryStore::default()),
        }
    }

    pub fn with_retry_store_arc(mut self, retries: Arc<dyn GapRetryStore>) -> Self {
        self.retries = retries;
        self
    }

    pub async fn run(self) -> PlatformResult<()> {
        let pending = self.load_pending().await?;
        Runtime::run_until_shutdown(self.process_gaps(pending)).await
    }

    async fn process_gaps(
        mut self,
        mut pending: BTreeMap<String, PendingGap>,
    ) -> PlatformResult<()> {
        let mut source_ended = false;

        loop {
            let now = TimestampMilliseconds::now();
            if let Some(key) = PendingGap::next_due_key(&pending, now) {
                if let Some(gap) = pending.remove(&key) {
                    let retry = gap.clone();
                    if let Err(error) = self.attempt(gap, &mut pending).await {
                        self.requeue_failed_retry(&error, retry, &mut pending).await;
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
                            self.requeue_failed_retry(&error, retry, &mut pending).await;
                        }
                    }
                }
                return Err(PlatformError::external(
                    "gap marker source ended before shutdown was requested",
                ));
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

    async fn requeue_failed_retry(
        &self,
        error: &PlatformError,
        gap: PendingGap,
        pending: &mut BTreeMap<String, PendingGap>,
    ) {
        MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_ERROR);
        warn!(%error, "failed to retry pending backfill range");
        let now = TimestampMilliseconds::now();
        let gap = pending.remove(&gap.key()).unwrap_or(gap);
        let gap = if gap.is_due(now) {
            gap.deferred(now)
        } else {
            gap
        };
        if let Err(error) = self.retries.save(&gap).await {
            warn!(
                %error,
                key = %gap.key(),
                "failed to persist delayed backfill retry"
            );
        }
        pending.insert(gap.key(), gap);
    }

    pub async fn load_pending(&self) -> PlatformResult<BTreeMap<String, PendingGap>> {
        let mut pending = BTreeMap::new();
        for gap in self.retries.load().await? {
            pending.insert(gap.key(), gap);
        }
        Ok(pending)
    }

    pub async fn accept(&mut self, gap: GapMarker, pending: &mut BTreeMap<String, PendingGap>) {
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
        let retry = initial.clone();
        pending.insert(key, initial.clone());

        if let Err(error) = self.attempt(initial, pending).await {
            self.requeue_failed_retry(&error, retry, pending).await;
        }
        if let Err(error) = self.source.ack(&gap).await {
            warn!(%error, "failed to acknowledge processed gap marker");
        }
    }
}
