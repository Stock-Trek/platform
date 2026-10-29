use crate::{
    config::WebhookConfig,
    dedup::DeliveryDedup,
    dto::event_converter::{EventConverter, EventConverters},
    secrets::{Secrets, SsmConfig},
};
use aws_sdk_sqs::Client as SqsClient;
use platform_aws::SdkConfig;
use platform_errors::{BpError, BpResult, SecretsError};
use platform_github::host_allow_list::HostAllowlist;
use platform_metrics::secret::SecretMetrics;
use platform_sqs::sqs::{Sqs, SqsConfig};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{Mutex, MutexGuard, RwLock};

const MAX_JITTER_RATIO: f64 = 0.5;
static JITTER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub type Converters = Arc<Vec<Box<dyn EventConverter>>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum LoadReason {
    CacheMiss,
    SignatureRefresh,
}

pub struct RsWebhookContext {
    pub sqs: Sqs,
    sdk_config: SdkConfig,
    timeouts: Timeouts,
    converters: RwLock<Option<Converters>>,
    dedup: DeliveryDedup,
    load: Mutex<()>,
    refreshed_at: Mutex<Option<Instant>>,
    backoff_until: Mutex<Option<Instant>>,
    refresh_cooldown: Duration,
    failure_backoff: Duration,
    deadline_buffer: Duration,
    ssm: SsmConfig,
    allowed_hosts: HostAllowlist,
}

impl RsWebhookContext {
    pub async fn new(config: &WebhookConfig) -> BpResult<Self> {
        let aws_timeouts = config.aws_timeouts;
        let sdk_config = platform_aws::load_sdk_config(aws_timeouts).await;
        let sqs = Sqs::new(
            SqsClient::new(&sdk_config),
            aws_timeouts,
            SqsConfig::new(config.sqs_queue_url.clone()),
        );
        let dedup = DeliveryDedup::new(config.dedup_max_deliveries, config.dedup_ttl);
        let context = Self {
            sqs,
            sdk_config,
            timeouts: aws_timeouts,
            converters: RwLock::new(None),
            dedup,
            load: Mutex::new(()),
            refreshed_at: Mutex::new(None),
            backoff_until: Mutex::new(None),
            refresh_cooldown: config.refresh_cooldown,
            failure_backoff: config.secret_failure_backoff,
            deadline_buffer: config.deadline_buffer,
            ssm: config.ssm.clone(),
            allowed_hosts: config.github_allowed_hosts.clone(),
        };
        // Warm the cache eagerly, but tolerate a transient SSM outage:
        // startup must not fail, and the first request will retry once the
        // failure backoff expires.
        let cold_start_refresh_timeout = config.cold_start_refresh_timeout;
        let deadline = Deadline::new(
            SystemTime::now() + cold_start_refresh_timeout,
            context.deadline_buffer,
        );
        if let Err(error) = context.converters(deadline).await {
            if !error.retryability().is_retryable() {
                return Err(error);
            }
            tracing::warn!("failed to load secrets during initialisation: {error}");
        }
        Ok(context)
    }

    pub fn dedup(&self) -> &DeliveryDedup {
        &self.dedup
    }

    pub fn deadline_buffer(&self) -> Duration {
        self.deadline_buffer
    }

    /// Returns the current converters, loading secrets on first use. Failed
    /// loads are throttled by a jittered backoff so a flood of unauthenticated
    /// webhooks cannot amplify into unbounded SSM traffic.
    pub async fn converters(&self, deadline: Deadline) -> BpResult<Converters> {
        if let Some(converters) = self.converters.read().await.clone() {
            return Ok(converters);
        }
        SecretMetrics::record_secret_cache_miss();
        self.load_converters(deadline, LoadReason::CacheMiss).await
    }

    /// Reloads secrets from SSM and rebuilds the converters, returning
    /// `None` when a refresh happened too recently. Serialising refreshes and
    /// rate limiting them stops forged webhooks from amplifying into a stream
    /// of outbound auth calls; callers should fall back to the cached
    /// converters (and reject the request) when `None` is returned.
    pub async fn refresh_converters(&self, deadline: Deadline) -> BpResult<Option<Converters>> {
        match self
            .load_converters(deadline, LoadReason::SignatureRefresh)
            .await
        {
            Ok(converters) => Ok(Some(converters)),
            Err(BpError::Secrets(SecretsError::RefreshThrottled)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn load_converters(
        &self,
        deadline: Deadline,
        reason: LoadReason,
    ) -> BpResult<Converters> {
        let _guard = self.acquire_load_lock().await;

        if reason == LoadReason::CacheMiss {
            if let Some(converters) = self.converters.read().await.clone() {
                return Ok(converters);
            }
        } else if self.refreshed_recently().await {
            return Err(BpError::Secrets(SecretsError::RefreshThrottled));
        }

        if let Some(retry_after) = self.remaining_backoff().await {
            SecretMetrics::record_secret_load_backoff_hit();
            return Err(BpError::Secrets(SecretsError::Unavailable { retry_after }));
        }

        SecretMetrics::record_secret_load_attempt();
        match self.try_load(deadline).await {
            Ok(converters) => {
                *self.converters.write().await = Some(converters.clone());
                *self.refreshed_at.lock().await = Some(Instant::now());
                *self.backoff_until.lock().await = None;
                SecretMetrics::record_secret_load_success();
                Ok(converters)
            }
            Err(error) => {
                SecretMetrics::record_secret_load_failure(error.error_type());
                tracing::warn!("failed to load secrets: {error}");
                if !error.retryability().is_retryable() {
                    return Err(error);
                }
                let retry_after = self.jittered_backoff();
                *self.backoff_until.lock().await = Some(Instant::now() + retry_after);
                Err(BpError::Secrets(SecretsError::Unavailable { retry_after }))
            }
        }
    }

    async fn try_load(&self, deadline: Deadline) -> BpResult<Converters> {
        let secrets = Secrets::load(&self.sdk_config, self.timeouts, deadline, &self.ssm).await?;
        Ok(Arc::new(EventConverters::all(
            &secrets,
            self.allowed_hosts.clone(),
        )?))
    }

    async fn acquire_load_lock(&self) -> MutexGuard<'_, ()> {
        match self.load.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                SecretMetrics::record_secret_load_in_flight_wait();
                self.load.lock().await
            }
        }
    }

    async fn refreshed_recently(&self) -> bool {
        self.refreshed_at
            .lock()
            .await
            .is_some_and(|instant| instant.elapsed() < self.refresh_cooldown)
    }

    async fn remaining_backoff(&self) -> Option<Duration> {
        let until = (*self.backoff_until.lock().await)?;
        let now = Instant::now();
        (now < until).then(|| until - now)
    }

    fn jittered_backoff(&self) -> Duration {
        let sequence = JITTER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let entropy = sequence.wrapping_mul(6364136223846793005).wrapping_add(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_nanos() as u64),
        );
        let fraction = (entropy >> 40) as f64 / f64::from(1u32 << 24);
        self.failure_backoff
            .mul_f64(1.0 + fraction * MAX_JITTER_RATIO)
    }
}
