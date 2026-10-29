use crate::config::AggregatorConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::{
    candle::{Candle, ClosedCandle},
    envelope::EnvelopeCodec,
};
use platform_nats::{JetStream, KeyValue, NatsPublisher};
use std::sync::Arc;
use tokio::sync::Mutex;

const NEXT_REVISION_KEY: &str = "next_closed_revision";
const PENDING_REVISION_KEY: &str = "pending_closed_revision";

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
/// Before a closed candle is published, it is stored as a pending revision in a
/// KV bucket. The revision counter is advanced only after JetStream acknowledges
/// the publish, and any pending revision is replayed on startup. This means a
/// crash at any point can neither duplicate nor skip a candle revision: an
/// acknowledged publish is committed, while an unacknowledged one is retried
/// with the same deterministic message id.
pub struct NatsCandleSink {
    publisher: NatsPublisher,
    open_candles: KeyValue,
    revisions: KeyValue,
    next_revision: Mutex<u64>,
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
        let sink = Self {
            publisher,
            open_candles,
            revisions,
            next_revision: Mutex::new(next_revision),
            closed_subject_prefix: config.closed_candle_subject_prefix.clone(),
        };
        sink.recover_pending().await?;
        Ok(sink)
    }

    async fn persist_next_revision(&self, next_revision: u64) -> BpResult<()> {
        self.revisions
            .put(NEXT_REVISION_KEY, next_revision.to_string().as_bytes())
            .await
            .map(|_| ())
    }

    /// Republishes a revision that was persisted but not fully committed before
    /// the previous process stopped, then advances the revision counter.
    async fn recover_pending(&self) -> BpResult<()> {
        let mut next_revision = self.next_revision.lock().await;
        self.recover_pending_locked(&mut next_revision).await
    }

    async fn recover_pending_locked(&self, next_revision: &mut u64) -> BpResult<()> {
        let Some(payload) = self.revisions.get_last(PENDING_REVISION_KEY).await? else {
            return Ok(());
        };
        let pending = ClosedCandle::decode(&payload)?;
        let (next_revision_value, already_committed) =
            Self::next_after_pending(*next_revision, pending.revision);

        // A counter above the pending revision means the publish was already
        // acknowledged before the pending marker could be removed. Only replay
        // truly uncommitted revisions.
        if !already_committed {
            let (subject, message_id) = self.publish_target(&pending);
            self.publisher
                .publish_with_message_id(&subject, &message_id, &payload)
                .await?;
            self.persist_next_revision(next_revision_value).await?;
            *next_revision = next_revision_value;
        }
        self.revisions.delete(PENDING_REVISION_KEY).await
    }

    fn next_after_pending(persisted_next: u64, pending_revision: u64) -> (u64, bool) {
        let already_committed = persisted_next > pending_revision;
        let next_revision = persisted_next.max(pending_revision.saturating_add(1));
        (next_revision, already_committed)
    }

    fn publish_target(&self, closed: &ClosedCandle) -> (String, String) {
        let subject = format!(
            "{}.{}",
            self.closed_subject_prefix,
            closed.candle.key.subject_suffix()
        );
        let message_id = format!("{subject}@{}", closed.candle.start_time().0);
        (subject, message_id)
    }

    fn parse_revision(value: &[u8]) -> Option<u64> {
        std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .filter(|revision| *revision > 0)
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
        let mut next_revision = self.next_revision.lock().await;
        self.recover_pending_locked(&mut next_revision).await?;
        for closed in candles {
            let revision = *next_revision;
            let pending = ClosedCandle {
                revision,
                candle: closed.candle.clone(),
            };
            let (subject, message_id) = self.publish_target(&pending);
            let payload = pending.encode()?;

            self.revisions.put(PENDING_REVISION_KEY, &payload).await?;
            self.publisher
                .publish_with_message_id(&subject, &message_id, &payload)
                .await?;
            self.persist_next_revision(revision.saturating_add(1))
                .await?;
            *next_revision = revision.saturating_add(1);
            self.revisions.delete(PENDING_REVISION_KEY).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_positive_revisions() {
        assert_eq!(NatsCandleSink::parse_revision(b"1"), Some(1));
        assert_eq!(NatsCandleSink::parse_revision(b" 42 "), Some(42));
        assert_eq!(NatsCandleSink::parse_revision(b"0"), None);
        assert_eq!(NatsCandleSink::parse_revision(b"not-a-number"), None);
    }

    #[test]
    fn an_uncommitted_pending_revision_is_advanced_and_replayed() {
        let (next_revision, already_committed) = NatsCandleSink::next_after_pending(7, 7);

        assert_eq!(next_revision, 8);
        assert!(!already_committed);
    }

    #[test]
    fn a_committed_pending_revision_is_not_replayed() {
        let (next_revision, already_committed) = NatsCandleSink::next_after_pending(8, 7);

        assert_eq!(next_revision, 8);
        assert!(already_committed);
    }

    #[test]
    fn recovery_never_rewinds_the_revision_counter() {
        let (next_revision, already_committed) = NatsCandleSink::next_after_pending(12, 7);

        assert_eq!(next_revision, 12);
        assert!(already_committed);
    }
}
