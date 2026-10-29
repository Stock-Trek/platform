use crate::sequence::{SequenceObservation, SequenceTracker};
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
use platform_nats::EventPublisher;
use platform_types::new_types::TimestampMilliseconds;
use std::{collections::HashMap, sync::Arc};
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
}

impl<P: EventPublisher> Ingester<P> {
    pub fn new(publisher: Arc<P>, options: IngestOptions) -> Self {
        Self {
            publisher,
            options,
            sequences: HashMap::new(),
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
            return self.publish_event(&event_subject, &event).await;
        };

        // `pu` is the final update ID of the preceding event. Checking it,
        // rather than assuming `U == previous u + 1`, avoids false gaps when
        // update IDs without book changes are intentionally skipped. Spot
        // depth events have no `pu`, so their `U` is the continuity boundary.
        let first_sequence = previous_sequence
            .and_then(|previous| previous.checked_add(1))
            .or(first_sequence)
            .unwrap_or(sequence);

        let observation = {
            let tracker = self.sequences.entry(event.stream.clone()).or_default();
            tracker.observe_range(first_sequence, sequence)
        };

        match observation {
            SequenceObservation::First | SequenceObservation::InOrder => {
                self.publish_event(&event_subject, &event).await
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
                self.publish_event(&event_subject, &event).await
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

    async fn publish_event(&self, subject: &str, event: &MarketEvent) -> BpResult<()> {
        self.publisher.publish(subject, &event.encode()?).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use platform_errors::BpResult;
    use serde_json::Value;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingPublisher {
        messages: Mutex<Vec<(String, Value)>>,
    }

    impl RecordingPublisher {
        fn messages(&self) -> Vec<(String, Value)> {
            self.messages.lock().expect("messages lock").clone()
        }
    }

    #[async_trait]
    impl EventPublisher for RecordingPublisher {
        async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
            let value = serde_json::from_slice(payload)?;
            self.messages
                .lock()
                .expect("messages lock")
                .push((subject.to_string(), value));
            Ok(())
        }
    }

    fn message(sequence: Option<u64>) -> ParsedMessage {
        ranged_message(sequence, None, None)
    }

    fn ranged_message(
        sequence: Option<u64>,
        first_sequence: Option<u64>,
        previous_sequence: Option<u64>,
    ) -> ParsedMessage {
        ParsedMessage {
            stream: "btcusdt@trade".to_string(),
            sequence,
            first_sequence,
            previous_sequence,
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
            payload: "eyJlIjoidHJhZGUifQ==".to_string(),
        }
    }

    fn ingester(publisher: Arc<RecordingPublisher>) -> Ingester<RecordingPublisher> {
        Ingester::new(
            publisher,
            IngestOptions {
                exchange: CexId::Binance,
                subject_prefix: "market".to_string(),
            },
        )
    }

    #[tokio::test]
    async fn publishes_events_in_sequence_order() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester.ingest(message(Some(1))).await.unwrap();
        ingester.ingest(message(Some(2))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, "market.events.binance.btcusdt.trade");
        assert_eq!(messages[1].1["sequence"], 2);
    }

    #[tokio::test]
    async fn publishes_a_gap_marker_before_the_observed_event() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester.ingest(message(Some(10))).await.unwrap();
        ingester.ingest(message(Some(13))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[1].1["missing_from"], 11);
        assert_eq!(messages[1].1["missing_to"], 12);
        assert_eq!(messages[1].1["observed_sequence"], 13);
        assert_eq!(messages[2].0, "market.events.binance.btcusdt.trade");
    }

    #[tokio::test]
    async fn drops_duplicate_events() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester.ingest(message(Some(10))).await.unwrap();
        ingester.ingest(message(Some(10))).await.unwrap();

        assert_eq!(publisher.messages().len(), 1);
    }

    #[tokio::test]
    async fn ranges_spanning_multiple_update_ids_do_not_create_false_gaps() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester
            .ingest(ranged_message(Some(100), Some(90), Some(89)))
            .await
            .unwrap();
        ingester
            .ingest(ranged_message(Some(110), Some(102), Some(100)))
            .await
            .unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1].1["sequence"], 110);
    }

    #[tokio::test]
    async fn previous_update_ids_report_only_the_missing_range() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester
            .ingest(ranged_message(Some(100), Some(90), Some(89)))
            .await
            .unwrap();
        ingester
            .ingest(ranged_message(Some(120), Some(116), Some(115)))
            .await
            .unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[1].1["missing_from"], 101);
        assert_eq!(messages[1].1["missing_to"], 115);
        assert_eq!(messages[1].1["observed_sequence"], 120);
    }

    #[tokio::test]
    async fn first_update_ids_detect_spot_depth_gaps() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester
            .ingest(ranged_message(Some(100), Some(90), None))
            .await
            .unwrap();
        ingester
            .ingest(ranged_message(Some(120), Some(110), None))
            .await
            .unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].1["missing_from"], 101);
        assert_eq!(messages[1].1["missing_to"], 109);
    }
}
