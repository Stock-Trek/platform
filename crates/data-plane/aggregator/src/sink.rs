use crate::config::AggregatorConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    candle::{Candle, ClosedCandle},
    envelope::EnvelopeCodec,
};
use platform_nats::{JetStream, KeyValue, NatsPublisher};
use std::sync::atomic::{AtomicU64, Ordering};

const NEXT_REVISION_KEY: &str = "next_closed_revision";

/// Stores the latest version of candles that are still open.
#[async_trait]
pub trait OpenCandleStore: Send + Sync {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()>;
}

/// Publishes candles after they close.
///
/// Implementations assign the authoritative, restart-stable revision to each
/// closed candle.
#[async_trait]
pub trait ClosedCandlePublisher: Send + Sync {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()>;
}

/// Stores open candles in a NATS KV bucket and publishes closed candles to
/// JetStream.
///
/// Open candles are written per key, so the latest state survives restarts.
/// Closed candles are published with an acknowledgement and a deterministic
/// message id, and the revision counter is persisted in a KV bucket before
/// each publish, so neither a reconnect nor a restart can duplicate or skip a
/// candle revision.
pub struct NatsCandleSink {
    publisher: NatsPublisher,
    open_candles: KeyValue,
    revisions: KeyValue,
    next_revision: AtomicU64,
    closed_subject_prefix: String,
}

impl NatsCandleSink {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let publisher =
            NatsPublisher::connect(&config.nats_url, "platform-aggregator-candles").await?;
        publisher
            .ensure_stream(&config.closed_candle_subject_prefix)
            .await?;
        let open_candles = KeyValue::connect(
            &config.nats_url,
            "platform-aggregator-candles",
            &JetStream::kv_bucket_name(&config.open_candle_subject_prefix),
        )
        .await?;
        let revisions = KeyValue::connect(
            &config.nats_url,
            "platform-aggregator-candles",
            &JetStream::kv_bucket_name(&config.closed_candle_subject_prefix),
        )
        .await?;
        let next_revision = revisions
            .get_last(NEXT_REVISION_KEY)
            .await?
            .and_then(|value| Self::parse_revision(&value))
            .unwrap_or(1);
        Ok(Self {
            publisher,
            open_candles,
            revisions,
            next_revision: AtomicU64::new(next_revision),
            closed_subject_prefix: config.closed_candle_subject_prefix.clone(),
        })
    }

    async fn persist_next_revision(&self, allocated_revision: u64) -> BpResult<()> {
        self.revisions
            .put(
                NEXT_REVISION_KEY,
                (allocated_revision + 1).to_string().as_bytes(),
            )
            .await
            .map(|_| ())
    }

    fn parse_revision(value: &[u8]) -> Option<u64> {
        std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .filter(|revision| *revision > 0)
    }
}

#[async_trait]
impl OpenCandleStore for NatsCandleSink {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        for candle in candles {
            let key = candle.key.subject_suffix();
            let payload = candle.encode()?;
            self.open_candles.put(&key, &payload).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for NatsCandleSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        for closed in candles {
            let revision = self.next_revision.fetch_add(1, Ordering::Relaxed);
            self.persist_next_revision(revision).await?;
            let subject = format!(
                "{}.{}",
                self.closed_subject_prefix,
                closed.candle.key.subject_suffix()
            );
            let message_id = format!("{subject}@{}", closed.candle.start_time().0);
            let payload = ClosedCandle {
                revision,
                candle: closed.candle.clone(),
            }
            .encode()?;
            self.publisher
                .publish_with_message_id(&subject, &message_id, &payload)
                .await?;
        }
        Ok(())
    }
}
