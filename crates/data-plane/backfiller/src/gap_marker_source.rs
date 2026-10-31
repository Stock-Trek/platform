use crate::config::BackfillerConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    envelope::EnvelopeCodec, event::GapMarker, subject::MarketEventSubject,
};
use platform_metrics::descriptions::{DECODE_KIND_GAP_MARKER, STAGE_BACKFILLER};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::{JetStreamConsumer, Message};
use tracing::warn;

const DURABLE_NAME: &str = "backfiller-gaps";

/// Source of gap markers emitted by the ingester.
#[async_trait]
pub trait GapMarkerSource: Send {
    /// Returns `None` when the source has reached its end. Sources that wait
    /// indefinitely for new gaps must keep waiting through idle periods instead
    /// of reporting `None`.
    async fn next_gap(&mut self) -> BpResult<Option<GapMarker>>;

    /// Acknowledges the gap returned by the previous call to [`Self::next_gap`].
    async fn ack(&mut self, _gap: &GapMarker) -> BpResult<()> {
        Ok(())
    }
}

/// Reads gap markers from a durable JetStream consumer.
///
/// Gap markers are acknowledged only after delivery to the backfiller, so
/// unprocessed gaps are redelivered after a restart instead of being lost.
pub struct NatsGapMarkerSource {
    gaps: JetStreamConsumer,
    in_flight: Option<Message>,
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
        Ok(Self {
            gaps,
            in_flight: None,
        })
    }
}

#[async_trait]
impl GapMarkerSource for NatsGapMarkerSource {
    async fn next_gap(&mut self) -> BpResult<Option<GapMarker>> {
        loop {
            // An expired fetch window means the gap marker stream is idle, not
            // that it has ended. Keep polling until a marker arrives or the
            // process is shut down.
            let Some(message) = self.gaps.next_message_unacked().await else {
                continue;
            };
            match GapMarker::decode(&message.payload) {
                Ok(gap) => {
                    self.in_flight = Some(message);
                    return Ok(Some(gap));
                }
                Err(error) => {
                    MarketDataMetrics::record_decode_failure(
                        STAGE_BACKFILLER,
                        DECODE_KIND_GAP_MARKER,
                    );
                    warn!(%error, "skipping malformed gap marker");
                    self.gaps.ack(&message).await?;
                }
            }
        }
    }

    async fn ack(&mut self, _gap: &GapMarker) -> BpResult<()> {
        let Some(message) = self.in_flight.take() else {
            return Ok(());
        };
        self.gaps.ack(&message).await
    }
}

#[cfg(test)]
mod tests;
