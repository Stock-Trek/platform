use crate::{
    fetcher::{BackfilledEvent, MissingEventFetcher},
    gap_marker_source::GapMarkerSource,
    publisher::EventPublisher,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use platform_errors::BpResult;
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
use platform_types::new_types::TimestampMilliseconds;
use serde_json::json;
use stock_trek_types::cex::cex_id::CexId;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct BackfillOptions {
    pub exchange: CexId,
    pub event_subject_prefix: String,
}

impl BackfillOptions {
    fn subject(&self, stream: &str) -> String {
        MarketEventSubject::new(&self.event_subject_prefix, self.exchange).event(stream)
    }
}

/// Turns gap markers into reconstructed market events on the ingester's event
/// subjects.
pub struct BackfillService<S, F, P> {
    source: S,
    fetcher: F,
    publisher: P,
    options: BackfillOptions,
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
        }
    }

    pub async fn run(mut self) -> BpResult<()> {
        loop {
            let next_gap = tokio::select! {
                gap = self.source.next_gap() => gap?,
                signal = platform_runtime::Runtime::shutdown_signal() => {
                    signal?;
                    return Ok(());
                }
            };
            let Some(gap) = next_gap else {
                info!("gap marker source ended");
                return Ok(());
            };
            if let Err(error) = self.backfill(gap).await {
                // Keep consuming gaps: one unsupported or temporarily unavailable
                // stream must not stop backfilling every other market.
                MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_ERROR);
                warn!(%error, "failed to backfill gap");
            }
        }
    }

    async fn backfill(&self, gap: GapMarker) -> BpResult<()> {
        if gap.exchange != self.options.exchange {
            // TODO: build a fetcher registry keyed by exchange instead of running
            // one backfiller process per exchange.
            return Ok(());
        }
        let expected = gap.missing_to.saturating_sub(gap.missing_from) + 1;
        let events = self.fetcher.fetch(&gap).await?;
        if events.is_empty() {
            MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_EMPTY);
            warn!(
                exchange = %gap.exchange,
                stream = %gap.stream,
                missing_from = gap.missing_from,
                missing_to = gap.missing_to,
                "exchange returned no events for gap"
            );
            return Ok(());
        }

        let mut published = 0_u64;
        for event in events {
            self.publish(&gap, event).await?;
            published += 1;
        }
        MarketDataMetrics::record_backfill_gap_coverage(expected, published);
        if published < expected {
            // TODO: persist unresolved ranges and retry them with a different REST
            // endpoint or a wider time window instead of only logging.
            MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_PARTIAL);
            warn!(
                exchange = %gap.exchange,
                stream = %gap.stream,
                expected,
                published,
                "backfill did not cover the whole gap"
            );
        } else {
            MarketDataMetrics::record_backfill_gap_outcome(BACKFILL_OUTCOME_COVERED);
        }
        info!(
            exchange = %gap.exchange,
            stream = %gap.stream,
            missing_from = gap.missing_from,
            missing_to = gap.missing_to,
            published,
            "backfilled gap"
        );
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
            received_at: TimestampMilliseconds(Utc::now().timestamp_millis()),
            payload,
        };
        self.publisher
            .publish(&self.options.subject(&gap.stream), &market_event.encode()?)
            .await
    }
}
