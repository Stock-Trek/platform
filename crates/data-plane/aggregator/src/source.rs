use crate::config::AggregatorConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    envelope::EnvelopeCodec, event::MarketEvent, subject::MarketEventSubject,
};
use platform_metrics::descriptions::{DECODE_KIND_MARKET_EVENT, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::{JetStreamConsumer, Message};
use tracing::warn;

const DURABLE_NAME: &str = "aggregator-events";

/// Source of ingester market events.
#[async_trait]
pub trait MarketEventSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>>;

    /// Acknowledges the event returned by the previous call to
    /// [`Self::next_event`].
    async fn ack(&mut self, _event: &MarketEvent) -> BpResult<()> {
        Ok(())
    }
}

/// Reads ingester events from a durable JetStream consumer.
///
/// Messages are acknowledged only after the aggregator has applied the event,
/// so unprocessed events are redelivered after a restart instead of being lost.
pub struct NatsMarketEventSource {
    events: JetStreamConsumer,
    in_flight: Option<Message>,
}

impl NatsMarketEventSource {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let events = JetStreamConsumer::connect(
            &config.nats_url,
            "platform-aggregator-events",
            &config.event_subject_prefix,
            &MarketEventSubject::prefix_wildcard(&config.event_subject_prefix),
            DURABLE_NAME,
        )
        .await?;
        Ok(Self {
            events,
            in_flight: None,
        })
    }
}

#[async_trait]
impl MarketEventSource for NatsMarketEventSource {
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
        loop {
            let Some(message) = self.events.next_message_unacked().await else {
                continue;
            };
            match MarketEvent::decode(&message.payload) {
                Ok(event) => {
                    self.in_flight = Some(message);
                    return Ok(Some(event));
                }
                Err(error) => {
                    MarketDataMetrics::record_decode_failure(
                        STAGE_AGGREGATOR,
                        DECODE_KIND_MARKET_EVENT,
                    );
                    warn!(%error, "skipping malformed market event");
                    self.events.ack(&message).await?;
                }
            }
        }
    }

    async fn ack(&mut self, _event: &MarketEvent) -> BpResult<()> {
        let Some(message) = self.in_flight.take() else {
            return Ok(());
        };
        self.events.ack(&message).await
    }
}

impl NatsMarketEventSource {
    #[cfg(test)]
    fn decode_event(message: Option<Message>) -> Option<MarketEvent> {
        let message = message?;
        match MarketEvent::decode(&message.payload) {
            Ok(event) => Some(event),
            Err(error) => {
                MarketDataMetrics::record_decode_failure(
                    STAGE_AGGREGATOR,
                    DECODE_KIND_MARKET_EVENT,
                );
                warn!(%error, "skipping malformed market event");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_types::new_types::TimestampMilliseconds;
    use stock_trek_types::cex::cex_id::CexId;

    fn event() -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(7),
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
            received_at: TimestampMilliseconds(1_700_000_000_001),
            payload: "eyJlIjoidHJhZGUifQ==".to_string(),
        }
    }

    #[test]
    fn decodes_valid_market_events() {
        let expected = event();
        let message = Message {
            payload: expected.encode().unwrap(),
            ..Message::default()
        };

        assert_eq!(
            NatsMarketEventSource::decode_event(Some(message)),
            Some(expected)
        );
    }

    #[test]
    fn skips_malformed_market_events() {
        let message = Message {
            payload: b"not json".to_vec(),
            ..Message::default()
        };

        assert_eq!(NatsMarketEventSource::decode_event(Some(message)), None);
    }
}
