use crate::config::AggregatorConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    envelope::EnvelopeCodec, event::MarketEvent, subject::MarketEventSubject,
};
use platform_nats::JetStreamConsumer;

const DURABLE_NAME: &str = "aggregator-events";

/// Source of ingester market events.
#[async_trait]
pub trait MarketEventSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>>;
}

/// Reads ingester events from a durable JetStream consumer.
///
/// Messages are explicitly acknowledged after delivery to the aggregator, so a
/// restart resumes from the last acknowledged event instead of skipping
/// everything published while the aggregator was down.
pub struct NatsMarketEventSource {
    events: JetStreamConsumer,
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
        Ok(Self { events })
    }
}

#[async_trait]
impl MarketEventSource for NatsMarketEventSource {
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
        loop {
            let Some(message) = self.events.next_message("market event").await else {
                continue;
            };
            return Ok(Some(MarketEvent::decode(&message.payload)?));
        }
    }
}
