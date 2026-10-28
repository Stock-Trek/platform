use crate::config::BackfillerConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    envelope::EnvelopeCodec, event::GapMarker, subject::MarketEventSubject,
};
use platform_nats::NatsSubscriber;

/// Source of gap markers emitted by the ingester.
#[async_trait]
pub trait GapMarkerSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_gap(&mut self) -> BpResult<Option<GapMarker>>;
}

/// Reads gap markers from a NATS wildcard subscription.
pub struct NatsGapMarkerSource {
    subscriber: NatsSubscriber,
}

impl NatsGapMarkerSource {
    pub async fn connect(config: &BackfillerConfig) -> BpResult<Self> {
        let subject =
            MarketEventSubject::new(&config.gap_subject_prefix, config.exchange).gap_wildcard();
        let subscriber =
            NatsSubscriber::connect(&config.nats_url, "platform-backfiller-gaps", &subject, "1")
                .await?;
        Ok(Self { subscriber })
    }
}

#[async_trait]
impl GapMarkerSource for NatsGapMarkerSource {
    async fn next_gap(&mut self) -> BpResult<Option<GapMarker>> {
        // TODO: persist the last processed gap marker or use a durable JetStream
        // consumer so gaps are retried after a restart.
        let Some((_subject, payload)) = self.subscriber.next_message().await? else {
            return Ok(None);
        };
        Ok(Some(GapMarker::decode(&payload)?))
    }
}
