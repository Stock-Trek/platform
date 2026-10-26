use crate::{
    event::{GapMarker, MarketEvent},
    publisher::EventPublisher,
    sequence::{SequenceObservation, SequenceTracker},
    source::ParsedMessage,
};
use chrono::Utc;
use platform_errors::BpResult;
use platform_types::new_types::TimestampMilliseconds;
use std::{collections::HashMap, sync::Arc};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub exchange: String,
    pub subject_prefix: String,
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
        let event = MarketEvent {
            exchange: self.options.exchange.clone(),
            stream: message.stream,
            sequence: message.sequence,
            exchange_timestamp: message.exchange_timestamp,
            received_at: TimestampMilliseconds(Utc::now().timestamp_millis()),
            payload: message.payload,
        };
        let event_subject = subject(
            &self.options.subject_prefix,
            "events",
            &self.options.exchange,
            &event.stream,
        );

        let Some(sequence) = event.sequence else {
            return self.publish_event(&event_subject, &event).await;
        };

        let observation = {
            let tracker = self.sequences.entry(event.stream.clone()).or_default();
            tracker.observe(sequence)
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
                    exchange: event.exchange.clone(),
                    stream: event.stream.clone(),
                    previous_sequence: missing_from - 1,
                    observed_sequence: sequence,
                    missing_from,
                    missing_to,
                    detected_at: event.received_at,
                };
                let gap_subject = subject(
                    &self.options.subject_prefix,
                    "gaps",
                    &self.options.exchange,
                    &event.stream,
                );
                self.publisher
                    .publish(&gap_subject, &serde_json::to_vec(&gap)?)
                    .await?;
                self.publish_event(&event_subject, &event).await
            }
            SequenceObservation::Duplicate | SequenceObservation::OutOfOrder => {
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
        self.publisher
            .publish(subject, &serde_json::to_vec(event)?)
            .await
    }
}

fn subject(prefix: &str, category: &str, exchange: &str, stream: &str) -> String {
    let stream = stream
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character.to_ascii_lowercase()
            } else {
                '.'
            }
        })
        .collect::<String>();
    format!("{prefix}.{category}.{exchange}.{stream}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use platform_errors::BpResult;
    use serde_json::{Value, json};
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
        ParsedMessage {
            stream: "btcusdt@trade".to_string(),
            sequence,
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
            payload: json!({ "e": "trade", "t": sequence }),
        }
    }

    fn ingester(publisher: Arc<RecordingPublisher>) -> Ingester<RecordingPublisher> {
        Ingester::new(
            publisher,
            IngestOptions {
                exchange: "binance".to_string(),
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
}
