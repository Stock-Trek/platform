use crate::config::BackfillerConfig;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::event::GapMarker;
use platform_nats::{JetStream, KeyValue, NatsSubject};
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::Mutex;
use tracing::warn;

pub(crate) const RETRY_DELAY: Duration = Duration::from_secs(5);
pub(crate) const MAX_RETRY_DELAY: Duration = Duration::from_secs(5 * 60);

/// A gap range that still needs data, together with its retry schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingGap {
    pub exchange: CexId,
    pub stream: String,
    pub missing_from: u64,
    pub missing_to: u64,
    pub attempts: u32,
    pub next_attempt_at: TimestampMilliseconds,
    pub last_error: Option<String>,
}

impl PendingGap {
    pub fn from_gap(gap: &GapMarker, next_attempt_at: TimestampMilliseconds) -> Self {
        Self {
            exchange: gap.exchange,
            stream: gap.stream.clone(),
            missing_from: gap.missing_from,
            missing_to: gap.missing_to,
            attempts: 0,
            next_attempt_at,
            last_error: None,
        }
    }

    pub fn from_range(
        gap: &GapMarker,
        missing_from: u64,
        missing_to: u64,
        attempts: u32,
        next_attempt_at: TimestampMilliseconds,
        last_error: Option<String>,
    ) -> Self {
        Self {
            exchange: gap.exchange,
            stream: gap.stream.clone(),
            missing_from,
            missing_to,
            attempts,
            next_attempt_at,
            last_error,
        }
    }

    pub fn key(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.exchange,
            NatsSubject::normalize_segment(&self.stream),
            self.missing_from,
            self.missing_to
        )
    }

    pub fn gap_marker(&self, detected_at: TimestampMilliseconds) -> GapMarker {
        GapMarker {
            exchange: self.exchange,
            stream: self.stream.clone(),
            previous_sequence: self.missing_from.saturating_sub(1),
            observed_sequence: self.missing_to.saturating_add(1),
            missing_from: self.missing_from,
            missing_to: self.missing_to,
            detected_at,
        }
    }

    pub fn is_due(&self, now: TimestampMilliseconds) -> bool {
        self.next_attempt_at <= now
    }

    pub fn retry_delay(attempts: u32) -> Duration {
        let exponent = attempts.min(16);
        RETRY_DELAY
            .saturating_mul(1_u32 << exponent)
            .min(MAX_RETRY_DELAY)
    }

    pub fn retry_at(now: TimestampMilliseconds, attempts: u32) -> TimestampMilliseconds {
        TimestampMilliseconds(now.0.saturating_add(
            i64::try_from(Self::retry_delay(attempts).as_millis()).unwrap_or(i64::MAX),
        ))
    }
}

#[async_trait]
pub trait GapRetryStore: Send + Sync {
    async fn load(&self) -> BpResult<Vec<PendingGap>>;

    async fn save(&self, pending: &PendingGap) -> BpResult<()>;

    async fn remove(&self, key: &str) -> BpResult<()>;
}

#[derive(Debug, Default)]
pub struct InMemoryGapRetryStore {
    pub(crate) pending: Mutex<HashMap<String, PendingGap>>,
}

impl InMemoryGapRetryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl GapRetryStore for InMemoryGapRetryStore {
    async fn load(&self) -> BpResult<Vec<PendingGap>> {
        Ok(self.pending.lock().await.values().cloned().collect())
    }

    async fn save(&self, pending: &PendingGap) -> BpResult<()> {
        self.pending
            .lock()
            .await
            .insert(pending.key(), pending.clone());
        Ok(())
    }

    async fn remove(&self, key: &str) -> BpResult<()> {
        self.pending.lock().await.remove(key);
        Ok(())
    }
}

pub struct NatsGapRetryStore {
    key_value: KeyValue,
}

impl NatsGapRetryStore {
    pub async fn connect(config: &BackfillerConfig) -> BpResult<Self> {
        let bucket =
            JetStream::kv_bucket_name(&format!("{}.backfill-retries", config.gap_subject_prefix));
        let key_value =
            KeyValue::connect(&config.nats_url, "platform-backfiller-retries", &bucket).await?;
        Ok(Self { key_value })
    }
}

#[async_trait]
impl GapRetryStore for NatsGapRetryStore {
    async fn load(&self) -> BpResult<Vec<PendingGap>> {
        let mut pending = Vec::new();
        for key in self.key_value.keys().await? {
            let Some(value) = self.key_value.get_last(&key).await? else {
                continue;
            };
            match serde_json::from_slice::<PendingGap>(&value) {
                Ok(gap) => pending.push(gap),
                Err(error) => warn!(%error, %key, "skipping malformed pending backfill range"),
            }
        }
        Ok(pending)
    }

    async fn save(&self, pending: &PendingGap) -> BpResult<()> {
        let value = serde_json::to_vec(pending)?;
        self.key_value.put(&pending.key(), &value).await?;
        Ok(())
    }

    async fn remove(&self, key: &str) -> BpResult<()> {
        self.key_value.delete(key).await
    }
}
