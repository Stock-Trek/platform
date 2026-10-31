use crate::sequence::{SequenceObservation, SequenceStore, SequenceTracker};
use platform_errors::BpResult;
use platform_market_data::{
    envelope::EnvelopeCodec,
    event::{GapMarker, MarketEvent, ParsedMessage},
    subject::MarketEventSubject,
};
use platform_metrics::descriptions::{
    DROP_REASON_DUPLICATE, DROP_REASON_OUT_OF_ORDER, STAGE_INGESTER,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::{EventPublisher, NatsSubject};
use platform_types::new_types::TimestampMilliseconds;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use stock_trek_types::cex::cex_id::CexId;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub exchange: CexId,
    pub subject_prefix: String,
}

impl IngestOptions {
    fn event_subject(&self, stream: &str) -> String {
        MarketEventSubject::new(&self.subject_prefix, self.exchange).event(stream)
    }

    fn gap_subject(&self, stream: &str) -> String {
        MarketEventSubject::new(&self.subject_prefix, self.exchange).gap(stream)
    }
}

pub struct Ingester<P> {
    publisher: Arc<P>,
    options: IngestOptions,
    sequences: HashMap<String, SequenceTracker>,
    loaded_streams: HashSet<String>,
    store: Arc<dyn SequenceStore>,
}

impl<P: EventPublisher> Ingester<P> {
    pub fn new(publisher: Arc<P>, options: IngestOptions, store: Arc<dyn SequenceStore>) -> Self {
        Self {
            publisher,
            options,
            sequences: HashMap::new(),
            loaded_streams: HashSet::new(),
            store,
        }
    }

    pub async fn ingest(&mut self, message: ParsedMessage) -> BpResult<()> {
        let ParsedMessage {
            stream,
            sequence,
            first_sequence,
            previous_sequence,
            exchange_timestamp,
            payload,
        } = message;
        let event = MarketEvent {
            exchange: self.options.exchange,
            stream,
            sequence,
            exchange_timestamp,
            received_at: TimestampMilliseconds::now(),
            payload,
        };
        let event_subject = self.options.event_subject(&event.stream);

        let Some(sequence) = event.sequence else {
            return self
                .publisher
                .publish(&event_subject, &event.encode()?)
                .await;
        };

        // `pu` is the final update ID of the preceding event. Checking it,
        // rather than assuming `U == previous u + 1`, avoids false gaps when
        // update IDs without book changes are intentionally skipped. Spot
        // depth events have no `pu`, so their `U` is the continuity boundary.
        let first_sequence = previous_sequence
            .and_then(|previous| previous.checked_add(1))
            .or(first_sequence)
            .unwrap_or(sequence)
            .min(sequence);

        let observation = {
            let tracker = self.sequence_tracker(&event.stream).await?;
            tracker.observe_range(first_sequence, sequence)
        };

        match observation {
            SequenceObservation::First | SequenceObservation::InOrder => {
                self.publish_event(&event_subject, &event, first_sequence, sequence)
                    .await
            }
            SequenceObservation::Gap {
                missing_from,
                missing_to,
            } => {
                let gap = GapMarker {
                    exchange: event.exchange,
                    stream: event.stream.clone(),
                    previous_sequence: missing_from - 1,
                    observed_sequence: sequence,
                    missing_from,
                    missing_to,
                    detected_at: event.received_at,
                };
                let gap_subject = self.options.gap_subject(&event.stream);
                self.publisher.publish(&gap_subject, &gap.encode()?).await?;

                // The gap marker covers this missing range even if publishing
                // the current event then fails.
                self.sequence_tracker(&event.stream)
                    .await?
                    .commit(missing_to);
                self.persist_sequence(&event.stream).await?;

                self.publish_event(&event_subject, &event, first_sequence, sequence)
                    .await
            }
            SequenceObservation::Duplicate | SequenceObservation::OutOfOrder => {
                let reason = match observation {
                    SequenceObservation::Duplicate => DROP_REASON_DUPLICATE,
                    _ => DROP_REASON_OUT_OF_ORDER,
                };
                MarketDataMetrics::record_dropped_event(STAGE_INGESTER, reason);
                debug!(
                    exchange = %event.exchange,
                    stream = %event.stream,
                    sequence,
                    "dropping duplicate or out-of-order event"
                );
                Ok(())
            }
        }
    }

    async fn publish_event(
        &mut self,
        subject: &str,
        event: &MarketEvent,
        first_sequence: u64,
        sequence: u64,
    ) -> BpResult<()> {
        let result = self.publisher.publish(subject, &event.encode()?).await;
        let tracker = self.sequence_tracker(&event.stream).await?;
        match &result {
            Ok(()) => tracker.commit(sequence),
            Err(_) => tracker.record_failure(first_sequence),
        }

        // Persist the post-publish boundary before the result is returned. If
        // this was the final publish before shutdown, the next process must
        // still know which range was not reliably published.
        self.persist_sequence(&event.stream).await?;
        result
    }

    async fn sequence_tracker(&mut self, stream: &str) -> BpResult<&mut SequenceTracker> {
        if !self.loaded_streams.contains(stream) {
            let key = self.sequence_key(stream);
            let tracker = self.store.load(&key).await?.unwrap_or_default();
            self.sequences.insert(stream.to_string(), tracker);
            self.loaded_streams.insert(stream.to_string());
        }
        self.sequences.get_mut(stream).ok_or_else(|| {
            platform_errors::BpError::bad_value(
                "sequence tracker",
                stream,
                "sequence tracker is not loaded",
            )
        })
    }

    async fn persist_sequence(&self, stream: &str) -> BpResult<()> {
        let Some(tracker) = self.sequences.get(stream) else {
            return Ok(());
        };
        self.store.save(&self.sequence_key(stream), tracker).await
    }

    fn sequence_key(&self, stream: &str) -> String {
        format!(
            "{}.{}",
            self.options.exchange,
            NatsSubject::normalize_segment(stream)
        )
    }
}
