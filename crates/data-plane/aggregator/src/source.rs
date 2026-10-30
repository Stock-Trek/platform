//! Input sources for the aggregator.
//!
//! The router consumes the ingester's market events and gap markers and
//! reshards them onto shard subjects. Each shard writer then consumes
//! only its own shard subject, which gives every candle key a single writer.

use crate::config::AggregatorConfig;
use crate::shard::ShardId;
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    envelope::EnvelopeCodec,
    event::{GapMarker, MarketEvent},
    subject::{EVENT_CATEGORY, GAP_CATEGORY},
};
use platform_metrics::descriptions::{
    DECODE_KIND_GAP_MARKER, DECODE_KIND_MARKET_EVENT, STAGE_AGGREGATOR,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::{ConsumerStart, JetStreamConsumer, Message};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// One input record consumed by the router or a shard writer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketInput {
    Event(MarketEvent),
    Gap(GapMarker),
}

impl MarketInput {
    pub fn event(event: MarketEvent) -> Self {
        Self::Event(event)
    }

    pub fn gap(gap: GapMarker) -> Self {
        Self::Gap(gap)
    }

    pub fn exchange(&self) -> stock_trek_types::cex::cex_id::CexId {
        match self {
            Self::Event(event) => event.exchange,
            Self::Gap(gap) => gap.exchange,
        }
    }

    pub fn is_gap(&self) -> bool {
        matches!(self, Self::Gap(_))
    }

    pub fn encode(&self) -> BpResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(payload: &[u8]) -> BpResult<Self> {
        Ok(serde_json::from_slice(payload)?)
    }

    /// Decodes an original market event or gap marker from a source message.
    ///
    /// The ingester subject contract puts the category (`events` or `gaps`) in
    /// the subject, so the payload does not need a discriminator.
    pub fn from_market_message(message: &Message) -> BpResult<Self> {
        if Self::is_gap_subject(&message.subject) {
            Ok(Self::Gap(GapMarker::decode(&message.payload)?))
        } else {
            Ok(Self::Event(MarketEvent::decode(&message.payload)?))
        }
    }

    pub fn is_gap_subject(subject: &str) -> bool {
        subject.split('.').any(|segment| segment == GAP_CATEGORY)
    }

    pub fn is_event_subject(subject: &str) -> bool {
        subject.split('.').any(|segment| segment == EVENT_CATEGORY)
    }
}

/// An input together with the source message that delivered it.
///
/// Keeping the whole message preserves the JetStream metadata needed to
/// checkpoint the source position before acknowledging it.
#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub input: MarketInput,
    pub message: Message,
}

impl SourceRecord {
    pub fn new(input: MarketInput, message: Message) -> Self {
        Self { input, message }
    }
}

/// A source of ordered aggregator inputs.
#[async_trait]
pub trait MarketSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_input(&mut self) -> BpResult<Option<SourceRecord>>;

    /// Acknowledges the record returned by the previous call to
    /// [`Self::next_input`]. It must only be called after the caller's state
    /// checkpoint has been saved.
    async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
        Ok(())
    }
}

/// Returns the longest subject prefix shared by `left` and `right`.
pub fn common_subject_prefix(left: &str, right: &str) -> Option<String> {
    let shared: Vec<&str> = left
        .split('.')
        .zip(right.split('.'))
        .take_while(|(left, right)| left == right)
        .map(|(segment, _)| segment)
        .collect();
    (!shared.is_empty()).then(|| shared.join("."))
}

/// Reads original market events and gap markers for the reshard router.
///
/// Both categories are consumed from one ordered stream so gaps are applied in
/// the same order the ingester observed them. The router must see a gap before
/// any later event in order to close the right candle window.
pub struct NatsMarketSource {
    consumer: JetStreamConsumer,
    in_flight: Option<Message>,
}

impl NatsMarketSource {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let root = common_subject_prefix(&config.event_subject_prefix, &config.gap_subject_prefix)
            .ok_or_else(|| {
                BpError::config_invalid(
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
    async fn next_input(&mut self) -> BpResult<Option<SourceRecord>> {
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

    async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
        let Some(message) = self.in_flight.take() else {
            return Ok(());
        };
        self.consumer.ack(&message).await
    }
}

/// Reads routed inputs for exactly one shard.
pub struct NatsShardSource {
    consumer: JetStreamConsumer,
    in_flight: Option<Message>,
}

impl NatsShardSource {
    /// Connects the shard durable consumer at the configured start
    /// position.
    pub async fn connect(
        config: &AggregatorConfig,
        shard: ShardId,
        start: ConsumerStart,
    ) -> BpResult<Self> {
        let shard_subject = shard.subject(&config.shard_subject_prefix);
        let filter = format!("{shard_subject}.>");
        let durable = config.shard_durable_name(shard);
        let consumer = JetStreamConsumer::connect_with_start(
            &config.nats_url,
            "platform-aggregator-shard",
            &config.shard_subject_prefix,
            &filter,
            &durable,
            start,
        )
        .await?;
        Ok(Self {
            consumer,
            in_flight: None,
        })
    }

    pub fn filter_subject(&self) -> &str {
        self.consumer.filter_subject()
    }

    /// Decodes a routed input payload. Exposed for source-level tests.
    pub fn decode_routed(payload: &[u8]) -> BpResult<MarketInput> {
        MarketInput::decode(payload)
    }
}

#[async_trait]
impl MarketSource for NatsShardSource {
    async fn next_input(&mut self) -> BpResult<Option<SourceRecord>> {
        loop {
            let Some(message) = self.consumer.next_message_unacked().await else {
                continue;
            };
            match Self::decode_routed(&message.payload) {
                Ok(input) => {
                    self.in_flight = Some(message.clone());
                    return Ok(Some(SourceRecord::new(input, message)));
                }
                Err(error) => {
                    MarketDataMetrics::record_decode_failure(
                        STAGE_AGGREGATOR,
                        DECODE_KIND_MARKET_EVENT,
                    );
                    warn!(%error, "skipping malformed routed aggregator input");
                    self.consumer.ack(&message).await?;
                }
            }
        }
    }

    async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
        let Some(message) = self.in_flight.take() else {
            return Ok(());
        };
        self.consumer.ack(&message).await
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

    fn message(subject: &str, payload: Vec<u8>) -> Message {
        Message {
            subject: subject.to_string(),
            payload,
            ..Message::default()
        }
    }

    #[test]
    fn decodes_market_events_from_event_subjects() {
        let expected = event();
        let message = message(
            "market.events.binance.btcusdt.trade",
            expected.encode().unwrap(),
        );

        assert_eq!(
            MarketInput::from_market_message(&message).unwrap(),
            MarketInput::Event(expected)
        );
    }

    #[test]
    fn decodes_gap_markers_from_gap_subjects() {
        let expected = GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 7,
            observed_sequence: 9,
            missing_from: 8,
            missing_to: 8,
            detected_at: TimestampMilliseconds(10),
        };
        let message = message(
            "market.gaps.binance.btcusdt.trade",
            expected.encode().unwrap(),
        );

        assert_eq!(
            MarketInput::from_market_message(&message).unwrap(),
            MarketInput::Gap(expected)
        );
        assert!(MarketInput::is_gap_subject(
            "market.gaps.binance.btcusdt.trade"
        ));
    }

    #[test]
    fn routed_inputs_round_trip() {
        let input = MarketInput::Event(event());

        assert_eq!(
            MarketInput::decode(&input.encode().unwrap()).unwrap(),
            input
        );
        assert_eq!(
            NatsShardSource::decode_routed(&input.encode().unwrap()).unwrap(),
            input
        );
    }

    #[test]
    fn malformed_payloads_are_rejected() {
        let message = message("market.events.binance.btcusdt.trade", b"not json".to_vec());
        assert!(MarketInput::from_market_message(&message).is_err());
    }

    #[test]
    fn event_and_gap_prefixes_share_the_stream_root() {
        assert_eq!(
            common_subject_prefix("market.events", "market.gaps").as_deref(),
            Some("market")
        );
        assert_eq!(
            common_subject_prefix("market.events.binance", "market.events").as_deref(),
            Some("market.events")
        );
        assert_eq!(common_subject_prefix("events", "gaps"), None);
    }
}
