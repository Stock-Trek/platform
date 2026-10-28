use crate::config::AggregatorConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    envelope::EnvelopeCodec, event::MarketEvent, subject::MarketEventSubject,
};
use platform_nats::NatsSubscriber;

/// Source of ingester market events.
#[async_trait]
pub trait MarketEventSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>>;
}

/// Reads ingester events from a NATS wildcard subscription.
pub struct NatsMarketEventSource {
    subscriber: NatsSubscriber,
}

impl NatsMarketEventSource {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let subscriber = NatsSubscriber::connect(
            &config.nats_url,
            "platform-aggregator-events",
            &MarketEventSubject::prefix_wildcard(&config.event_subject_prefix),
            "1",
        )
        .await?;
        Ok(Self { subscriber })
    }
}

#[async_trait]
impl MarketEventSource for NatsMarketEventSource {
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
        let Some((_subject, payload)) = self.subscriber.next_message().await? else {
            return Ok(None);
        };
        Ok(Some(MarketEvent::decode(&payload)?))
    }
}
