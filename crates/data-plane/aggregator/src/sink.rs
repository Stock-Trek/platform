use crate::config::AggregatorConfig;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    candle::{Candle, CandleKey, ClosedCandle},
    envelope::EnvelopeCodec,
};
use platform_nats::{JetStream, KeyValue, NatsPublisher};
use std::collections::HashMap;
use std::sync::Arc;

/// Maximum number of independent sink operations in flight at once.
///
/// Operations for one candle key are kept ordered; the bound applies across
/// independent keys so a large flush cannot overwhelm a single stream leader.
const MAX_CONCURRENT_SINK_OPERATIONS: usize = 16;

/// Stores the latest version of candles that are still open.
///
/// This is a derived serving projection. Recovery uses the keyed state stored in
/// shard checkpoints, not this bucket.
#[async_trait]
pub trait OpenCandleStore: Send + Sync {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()>;

    /// Removes open-candle entries whose windows have closed and that no longer
    /// have a newer open window for the same candle key.
    async fn delete_open(&self, keys: &[CandleKey]) -> BpResult<()> {
        let _ = keys;
        Ok(())
    }
}

/// Publishes candles after they close.
#[async_trait]
pub trait ClosedCandlePublisher: Send + Sync {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()>;
}

/// Stores open candles in a NATS KV bucket and publishes closed candles to
/// JetStream.
///
/// Open candles are written per key only so clients can serve the current
/// partial candle. When the last open window for a key closes, that key is
/// tombstoned in the bucket so consumers cannot serve a stale partial candle.
/// They are a derived projection of the checkpointed keyed state; recovery
/// never reads this bucket back into the aggregator.
///
/// Closed candles already carry a deterministic revision derived from the
/// candle identity, window, close watermark, source offset, and shard state
/// epoch. Publishing therefore needs no global revision counter or pending
/// marker: every replay publishes the same `Nats-Msg-Id`, which JetStream
/// de-duplicates while the shard checkpoint still fences redelivery.
pub struct NatsCandleSink {
    publisher: NatsPublisher,
    open_candles: KeyValue,
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
        Ok(Self {
            publisher,
            open_candles,
            closed_subject_prefix: config.closed_candle_subject_prefix.clone(),
        })
    }

    fn publish_target(prefix: &str, closed: &ClosedCandle) -> (String, String) {
        let subject = format!("{}.{}", prefix, closed.candle.key.subject_suffix());
        (subject, closed.message_id())
    }
}

#[async_trait]
impl<T> OpenCandleStore for Arc<T>
where
    T: OpenCandleStore + ?Sized,
{
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        (**self).update_open(candles).await
    }

    async fn delete_open(&self, keys: &[CandleKey]) -> BpResult<()> {
        (**self).delete_open(keys).await
    }
}

#[async_trait]
impl<T> ClosedCandlePublisher for Arc<T>
where
    T: ClosedCandlePublisher + ?Sized,
{
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        (**self).publish_closed(candles).await
    }
}

#[async_trait]
impl OpenCandleStore for NatsCandleSink {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        stream::iter(candles.iter().cloned())
            .map(|candle| async move {
                let key = candle.key.subject_suffix();
                let payload = candle.encode()?;
                self.open_candles.put(&key, &payload).await.map(|_| ())
            })
            .buffer_unordered(MAX_CONCURRENT_SINK_OPERATIONS)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    async fn delete_open(&self, keys: &[CandleKey]) -> BpResult<()> {
        stream::iter(keys.iter().cloned())
            .map(|key| async move { self.open_candles.delete(&key.subject_suffix()).await })
            .buffer_unordered(MAX_CONCURRENT_SINK_OPERATIONS)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for NatsCandleSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        // Group by candle key so revisions for the same key are published in
        // input order, while independent keys can be published concurrently.
        let mut groups: HashMap<CandleKey, Vec<ClosedCandle>> = HashMap::new();
        for closed in candles {
            groups
                .entry(closed.candle.key.clone())
                .or_default()
                .push(closed.clone());
        }

        stream::iter(groups.into_values())
            .map(|group| async move {
                for closed in group {
                    let (subject, message_id) =
                        Self::publish_target(&self.closed_subject_prefix, &closed);
                    let payload = closed.encode()?;
                    self.publisher
                        .publish_with_message_id(&subject, &message_id, &payload)
                        .await?;
                }
                Ok::<(), BpError>(())
            })
            .buffer_unordered(MAX_CONCURRENT_SINK_OPERATIONS)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
