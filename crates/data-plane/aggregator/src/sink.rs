use crate::{aggregator::ClosedCandle, config::AggregatorConfig};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::candle::Candle;
use platform_nats::NatsPublisher;
use serde::Serialize;

/// Stores the latest version of candles that are still open.
#[async_trait]
pub trait OpenCandleStore: Send + Sync {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()>;
}

/// Publishes candles after they close.
#[async_trait]
pub trait ClosedCandlePublisher: Send + Sync {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()>;
}

/// Publishes open and closed candles to NATS subjects.
///
/// TODO: open candles should be written to a NATS KV bucket, and closed candles
/// should use JetStream acknowledgements with a durable message id. Core NATS
/// publishing is used here as a first-cut transport.
pub struct NatsCandleSink {
    publisher: NatsPublisher,
    open_subject_prefix: String,
    closed_subject_prefix: String,
}

impl NatsCandleSink {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let publisher =
            NatsPublisher::connect(&config.nats_url, "platform-aggregator-candles").await?;
        Ok(Self {
            publisher,
            open_subject_prefix: config.open_candle_subject_prefix.clone(),
            closed_subject_prefix: config.closed_candle_subject_prefix.clone(),
        })
    }

    async fn publish<T: Serialize>(&self, prefix: &str, suffix: &str, value: &T) -> BpResult<()> {
        let subject = format!("{prefix}.{suffix}");
        self.publisher
            .publish(&subject, &serde_json::to_vec(value)?)
            .await
    }
}

#[async_trait]
impl OpenCandleStore for NatsCandleSink {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        for candle in candles {
            self.publish(
                &self.open_subject_prefix,
                &candle.key.subject_suffix(),
                candle,
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for NatsCandleSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        for closed in candles {
            self.publish(
                &self.closed_subject_prefix,
                &closed.candle.key.subject_suffix(),
                closed,
            )
            .await?;
        }
        Ok(())
    }
}
