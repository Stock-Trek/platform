use crate::{
    config::AggregatorConfig, market_input::MarketInput, market_source::MarketSource,
    source_record::SourceRecord,
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::MarketDataMetrics;
use platform_metrics::{DECODE_KIND_GAP_MARKER, DECODE_KIND_MARKET_EVENT, STAGE_AGGREGATOR};
use platform_nats::{JetStreamConsumer, Message};
use tracing::warn;

pub struct NatsMarketSource {
    consumer: JetStreamConsumer,
    in_flight: Option<Message>,
}

impl NatsMarketSource {
    pub fn common_subject_prefix(left: &str, right: &str) -> Option<String> {
        let shared: Vec<&str> = left
            .split('.')
            .zip(right.split('.'))
            .take_while(|(left, right)| left == right)
            .map(|(segment, _)| segment)
            .collect();
        (!shared.is_empty()).then(|| shared.join("."))
    }

    pub async fn connect(config: &AggregatorConfig) -> PlatformResult<Self> {
        let root =
            Self::common_subject_prefix(&config.event_subject_prefix, &config.gap_subject_prefix)
                .ok_or_else(|| {
                PlatformError::config_invalid(
                    "aggregator subject prefixes",
                    "event and gap prefixes must share a common root",
                )
            })?;
        let filter = format!("{root}.>");
        let consumer = JetStreamConsumer::connect(
            &config.nats_url,
            "platform-aggregator-router",
            &root,
            &filter,
            &config.router_durable_name(),
        )
        .await?;
        Ok(Self {
            consumer,
            in_flight: None,
        })
    }
}

#[async_trait]
impl MarketSource for NatsMarketSource {
    async fn next_input(&mut self) -> PlatformResult<Option<SourceRecord>> {
        loop {
            let Some(message) = self.consumer.next_message_unacked().await else {
                continue;
            };
            if !MarketInput::is_event_subject(&message.subject)
                && !MarketInput::is_gap_subject(&message.subject)
            {
                warn!(subject = %message.subject, "skipping non-aggregator subject");
                self.consumer.ack(&message).await?;
                continue;
            }
            match MarketInput::from_market_message(&message) {
                Ok(input) => {
                    self.in_flight = Some(message.clone());
                    return Ok(Some(SourceRecord::new(input, message)));
                }
                Err(error) => {
                    let kind = if MarketInput::is_gap_subject(&message.subject) {
                        DECODE_KIND_GAP_MARKER
                    } else {
                        DECODE_KIND_MARKET_EVENT
                    };
                    MarketDataMetrics::record_decode_failure(STAGE_AGGREGATOR, kind);
                    warn!(%error, subject = %message.subject, "skipping malformed aggregator input");
                    self.consumer.ack(&message).await?;
                }
            }
        }
    }

    async fn ack(&mut self, _record: &SourceRecord) -> PlatformResult<()> {
        let Some(message) = self.in_flight.take() else {
            return Ok(());
        };
        self.consumer.ack(&message).await
    }
}
