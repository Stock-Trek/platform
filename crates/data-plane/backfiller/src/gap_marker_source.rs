use crate::config::BackfillerConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    envelope::EnvelopeCodec, event::GapMarker, subject::MarketEventSubject,
};
use platform_nats::JetStreamConsumer;

const DURABLE_NAME: &str = "backfiller-gaps";

/// Source of gap markers emitted by the ingester.
#[async_trait]
pub trait GapMarkerSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_gap(&mut self) -> BpResult<Option<GapMarker>>;
}

/// Reads gap markers from a durable JetStream consumer.
///
/// Gap markers are acknowledged only after delivery to the backfiller, so
/// unprocessed gaps are redelivered after a restart instead of being lost.
pub struct NatsGapMarkerSource {
    gaps: JetStreamConsumer,
}

impl NatsGapMarkerSource {
    pub async fn connect(config: &BackfillerConfig) -> BpResult<Self> {
        let subject =
            MarketEventSubject::new(&config.gap_subject_prefix, config.exchange).gap_wildcard();
        let durable = format!("{DURABLE_NAME}-{}", config.exchange).to_lowercase();
        let gaps = JetStreamConsumer::connect(
            &config.nats_url,
            "platform-backfiller-gaps",
            &config.gap_subject_prefix,
            &subject,
            &durable,
        )
        .await?;
        Ok(Self { gaps })
    }
}

#[async_trait]
impl GapMarkerSource for NatsGapMarkerSource {
    async fn next_gap(&mut self) -> BpResult<Option<GapMarker>> {
        loop {
            let Some(message) = self.gaps.next_message("gap marker").await else {
                continue;
            };
            return Ok(Some(GapMarker::decode(&message.payload)?));
        }
    }
}
