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
            let message = self.gaps.next_message("gap marker").await;
            if let Some(gap) = Self::decode_gap(message) {
                return Ok(Some(gap));
            }
        }
    }
}

impl NatsGapMarkerSource {
    fn decode_gap(message: Option<Message>) -> Option<GapMarker> {
        let message = message?;
        match GapMarker::decode(&message.payload) {
            Ok(gap) => Some(gap),
            Err(error) => {
                MarketDataMetrics::record_decode_failure(STAGE_BACKFILLER, DECODE_KIND_GAP_MARKER);
                warn!(%error, "skipping malformed gap marker");
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

    fn gap() -> GapMarker {
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 6,
            observed_sequence: 8,
            missing_from: 7,
            missing_to: 7,
            detected_at: TimestampMilliseconds(1_700_000_000_002),
        }
    }

    #[test]
    fn decodes_valid_gap_markers() {
        let expected = gap();
        let message = Message {
            payload: expected.encode().unwrap(),
            ..Message::default()
        };

        assert_eq!(
            NatsGapMarkerSource::decode_gap(Some(message)),
            Some(expected)
        );
    }

    #[test]
    fn skips_malformed_gap_markers() {
        let message = Message {
            payload: b"not json".to_vec(),
            ..Message::default()
        };

        assert_eq!(NatsGapMarkerSource::decode_gap(Some(message)), None);
    }
}
