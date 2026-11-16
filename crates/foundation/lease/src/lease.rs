use crate::{LeaseStore, LeaseWatch};
use platform_errors::{PlatformError, PlatformResult};
use platform_time::RetryBackoff;
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::warn;

const LEASE_KEY_PREFIX: &str = "lease";
const WATCH_ERROR_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseScope(String);

impl LeaseScope {
    pub fn new(scope: impl Into<String>) -> Self {
        Self(scope.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn key(&self) -> String {
        let digest = Sha256::digest(self.0.as_bytes());
        format!(
            "{LEASE_KEY_PREFIX}-{}",
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub scope: LeaseScope,
    pub owner_id: String,
    pub fencing_token: u64,
    pub expires_at: TimestampMilliseconds,
    #[serde(default)]
    pub clock_skew_ms: u64,
}

impl Lease {
    pub fn is_expired(&self, now: TimestampMilliseconds) -> bool {
        let skew = i64::try_from(self.clock_skew_ms).unwrap_or(i64::MAX);
        self.expires_at.saturating_add(DurationMilliseconds(skew)) < now
    }

    pub fn is_held_by(&self, owner_id: &str, fencing_token: u64) -> bool {
        !self.owner_id.is_empty()
            && self.owner_id == owner_id
            && self.fencing_token == fencing_token
    }

    pub fn is_released(&self) -> bool {
        self.owner_id.is_empty()
    }

    pub fn expires_at(now: TimestampMilliseconds, ttl: Duration) -> TimestampMilliseconds {
        let millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        now.saturating_add(DurationMilliseconds(millis))
    }

    pub fn released(scope: LeaseScope, fencing_token: u64, clock_skew_ms: u64) -> Self {
        Self {
            scope,
            owner_id: String::new(),
            fencing_token,
            expires_at: TimestampMilliseconds(0),
            clock_skew_ms,
        }
    }

    pub fn remaining(&self, now: TimestampMilliseconds) -> Duration {
        let skew = i64::try_from(self.clock_skew_ms).unwrap_or(i64::MAX);
        let expires_at = self.expires_at.saturating_add(DurationMilliseconds(skew));
        let remaining = expires_at.saturating_duration_since(now);
        let millis = u64::try_from(remaining.0.max(1)).unwrap_or(u64::MAX);
        Duration::from_millis(millis)
    }

    pub fn new_owner_id() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!(
            "{}-{}-{}",
            std::process::id(),
            TimestampMilliseconds::now().0,
            counter
        )
    }

    pub async fn wait_for_lease(
        store: &dyn LeaseStore,
        request: &LeaseRequest,
    ) -> PlatformResult<Self> {
        let mut watch: Option<LeaseWatch> = None;
        let mut watch_backoff = RetryBackoff::constant(WATCH_ERROR_BACKOFF).without_jitter();
        loop {
            if let Some(lease) = store.acquire(request).await? {
                return Ok(lease);
            }
            let current = store.current(&request.scope).await?;
            let now = TimestampMilliseconds::now();
            let Some(current) = current else {
                continue;
            };
            if current.is_expired(now) {
                continue;
            }
            if watch.is_none() {
                watch = Some(store.watch(&request.scope)?);
            }
            let delay = current.remaining(now);
            let watch = watch.as_mut().expect("lease watch is connected");
            tokio::select! {
                change = watch.next() => {
                    if let Err(error) = change {
                        warn!(
                            %error,
                            table = request.context.table(),
                            scope = %request.scope.as_str(),
                            owner = %request.owner_id,
                            source_batch_digest = request.context.source_batch_digest(),
                            "lease watch failed; retrying"
                        );
                        tokio::time::sleep(watch_backoff.next_delay()).await;
                    }
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LeaseContext {
    pub table: Option<String>,
    pub source_batch_digest: Option<String>,
}

impl LeaseContext {
    pub fn table(&self) -> &str {
        self.table.as_deref().unwrap_or("")
    }

    pub fn source_batch_digest(&self) -> &str {
        self.source_batch_digest.as_deref().unwrap_or("")
    }
}

#[derive(Debug, Clone)]
pub struct LeaseRequest {
    pub scope: LeaseScope,
    pub owner_id: String,
    pub ttl: Duration,
    pub initial_token: u64,
    pub clock_skew: Duration,
    pub context: LeaseContext,
}

impl LeaseRequest {
    pub fn new(scope: LeaseScope, owner_id: impl Into<String>, ttl: Duration) -> Self {
        Self {
            scope,
            owner_id: owner_id.into(),
            ttl,
            initial_token: 1,
            clock_skew: Duration::ZERO,
            context: LeaseContext::default(),
        }
    }

    pub fn with_clock_skew(mut self, clock_skew: Duration) -> Self {
        self.clock_skew = clock_skew;
        self
    }

    pub fn clock_skew_millis(&self) -> u64 {
        u64::try_from(self.clock_skew.as_millis()).unwrap_or(u64::MAX)
    }

    pub fn acquire_now(&self) -> TimestampMilliseconds {
        TimestampMilliseconds(
            TimestampMilliseconds::now()
                .0
                .saturating_sub(i64::try_from(self.clock_skew_millis()).unwrap_or(i64::MAX)),
        )
    }

    pub fn with_context(
        mut self,
        table: impl Into<String>,
        source_batch_digest: impl Into<String>,
    ) -> Self {
        self.context = LeaseContext {
            table: Some(table.into()),
            source_batch_digest: Some(source_batch_digest.into()),
        };
        self
    }

    pub fn with_initial_token(mut self, initial_token: i64) -> PlatformResult<Self> {
        self.initial_token = Self::validate_initial_token(initial_token)?;
        Ok(self)
    }

    pub fn validate_initial_token(initial_token: i64) -> PlatformResult<u64> {
        u64::try_from(initial_token)
            .ok()
            .filter(|token| *token > 0)
            .ok_or_else(|| {
                PlatformError::config_invalid("lease initial token", "must be a positive integer")
            })
    }

    pub fn renewal_interval(ttl: Duration) -> PlatformResult<Duration> {
        let interval = Duration::from_millis(u64::try_from(ttl.as_millis() / 3).map_err(|_| {
            PlatformError::config_invalid("lease TTL", "is too large to represent")
        })?);
        if interval.is_zero() {
            return Err(PlatformError::config_invalid(
                "lease TTL",
                "must produce a positive renewal interval",
            ));
        }
        Ok(interval)
    }
}
