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
        let tracker = self
            .sequences
            .get_mut(&event.stream)
            .expect("sequence tracker is loaded before publishing");
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
        Ok(self
            .sequences
            .get_mut(stream)
            .expect("sequence tracker is loaded"))
    }

    async fn persist_sequence(&self, stream: &str) -> BpResult<()> {
        let tracker = self
            .sequences
            .get(stream)
            .expect("sequence tracker is loaded before persistence");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequence::InMemorySequenceStore;
    use async_trait::async_trait;
    use platform_errors::BpResult;
    use serde_json::Value;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingPublisher {
        messages: Mutex<Vec<(String, Value)>>,
        fail_sequences: Mutex<Vec<u64>>,
    }

    impl RecordingPublisher {
        fn messages(&self) -> Vec<(String, Value)> {
            self.messages.lock().expect("messages lock").clone()
        }

        fn fail_sequence(&self, sequence: u64) {
            self.fail_sequences
                .lock()
                .expect("fail sequences lock")
                .push(sequence);
        }
    }

    #[async_trait]
    impl EventPublisher for RecordingPublisher {
        async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
            let value: Value = serde_json::from_slice(payload)?;
            let should_fail =
                value
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .is_some_and(|sequence| {
                        let mut failures = self.fail_sequences.lock().expect("fail sequences lock");
                        if let Some(index) = failures.iter().position(|failed| *failed == sequence)
                        {
                            failures.remove(index);
                            true
                        } else {
                            false
                        }
                    });
            if should_fail {
                return Err(platform_errors::BpError::bad_value(
                    "publish",
                    subject,
                    "injected publish failure",
                ));
            }
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
        Ingester::new(publisher, options(), Arc::new(InMemorySequenceStore::new()))
    }

    fn ingester_with_store(
        publisher: Arc<RecordingPublisher>,
        store: Arc<InMemorySequenceStore>,
    ) -> Ingester<RecordingPublisher> {
        Ingester::new(publisher, options(), store)
    }

    fn options() -> IngestOptions {
        IngestOptions {
            exchange: CexId::Binance,
            subject_prefix: "market".to_string(),
        }
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
    async fn a_failed_publish_creates_a_gap_for_the_next_event() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester.ingest(message(Some(10))).await.unwrap();
        publisher.fail_sequence(11);
        assert!(ingester.ingest(message(Some(11))).await.is_err());
        ingester.ingest(message(Some(12))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[1].1["missing_from"], 11);
        assert_eq!(messages[1].1["missing_to"], 11);
        assert_eq!(messages[1].1["observed_sequence"], 12);
        assert_eq!(messages[2].0, "market.events.binance.btcusdt.trade");
        assert_eq!(messages[2].1["sequence"], 12);
    }

    #[tokio::test]
    async fn a_failed_first_publish_creates_a_gap_for_the_next_event() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        publisher.fail_sequence(10);
        assert!(ingester.ingest(message(Some(10))).await.is_err());
        ingester.ingest(message(Some(11))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[0].1["missing_from"], 10);
        assert_eq!(messages[0].1["missing_to"], 10);
        assert_eq!(messages[1].1["sequence"], 11);
    }

    #[tokio::test]
    async fn a_failed_event_after_a_gap_is_not_suppressed() {
        let publisher = Arc::new(RecordingPublisher::default());
        let mut ingester = ingester(publisher.clone());

        ingester.ingest(message(Some(10))).await.unwrap();
        publisher.fail_sequence(13);
        assert!(ingester.ingest(message(Some(13))).await.is_err());
        ingester.ingest(message(Some(14))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1].1["missing_from"], 11);
        assert_eq!(messages[1].1["missing_to"], 12);
        assert_eq!(messages[2].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[2].1["missing_from"], 13);
        assert_eq!(messages[2].1["missing_to"], 13);
        assert_eq!(messages[3].1["sequence"], 14);
    }

    #[tokio::test]
    async fn restart_detects_gaps_that_happened_while_the_ingester_was_down() {
        let publisher = Arc::new(RecordingPublisher::default());
        let store = Arc::new(InMemorySequenceStore::new());

        {
            let mut ingester = ingester_with_store(publisher.clone(), store.clone());
            ingester.ingest(message(Some(10))).await.unwrap();
        }

        let mut restarted = ingester_with_store(publisher.clone(), store);
        restarted.ingest(message(Some(13))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[1].1["missing_from"], 11);
        assert_eq!(messages[1].1["missing_to"], 12);
        assert_eq!(messages[1].1["observed_sequence"], 13);
        assert_eq!(messages[2].1["sequence"], 13);
    }

    #[tokio::test]
    async fn restart_after_a_failed_final_publish_detects_the_gap() {
        let publisher = Arc::new(RecordingPublisher::default());
        let store = Arc::new(InMemorySequenceStore::new());

        {
            let mut ingester = ingester_with_store(publisher.clone(), store.clone());
            ingester.ingest(message(Some(10))).await.unwrap();
            publisher.fail_sequence(11);
            assert!(ingester.ingest(message(Some(11))).await.is_err());
        }

        let mut restarted = ingester_with_store(publisher.clone(), store);
        restarted.ingest(message(Some(12))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[1].1["missing_from"], 11);
        assert_eq!(messages[1].1["missing_to"], 11);
        assert_eq!(messages[1].1["observed_sequence"], 12);
        assert_eq!(messages[2].1["sequence"], 12);
    }

    #[tokio::test]
    async fn restart_after_a_failed_first_publish_detects_the_gap() {
        let publisher = Arc::new(RecordingPublisher::default());
        let store = Arc::new(InMemorySequenceStore::new());

        {
            let mut ingester = ingester_with_store(publisher.clone(), store.clone());
            publisher.fail_sequence(10);
            assert!(ingester.ingest(message(Some(10))).await.is_err());
        }

        let mut restarted = ingester_with_store(publisher.clone(), store);
        restarted.ingest(message(Some(11))).await.unwrap();

        let messages = publisher.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, "market.gaps.binance.btcusdt.trade");
        assert_eq!(messages[0].1["missing_from"], 10);
        assert_eq!(messages[0].1["missing_to"], 10);
        assert_eq!(messages[1].1["sequence"], 11);
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
