use super::*;
use crate::sequence::tests::InMemorySequenceStore;
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
        let should_fail = value
            .get("sequence")
            .and_then(Value::as_u64)
            .is_some_and(|sequence| {
                let mut failures = self.fail_sequences.lock().expect("fail sequences lock");
                if let Some(index) = failures.iter().position(|failed| *failed == sequence) {
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
