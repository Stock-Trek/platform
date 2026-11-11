use crate::ValkeyReplayStoreConfig;
use platform_errors::{BpError, BpResult};
use std::{fmt, time::Duration};

pub const DEFAULT_TOKEN_USAGE_QUEUE_PREFIX: &str = "auth-usage";
pub const DEFAULT_TOKEN_USAGE_QUEUE_CAPACITY: usize = 10_000;
pub const DEFAULT_TOKEN_USAGE_QUEUE_BATCH_SIZE: usize = 100;

#[derive(Clone)]
pub struct TokenUsageQueueConfig {
    valkey: ValkeyReplayStoreConfig,
    capacity: usize,
    batch_size: usize,
}

impl TokenUsageQueueConfig {
    pub fn new(
        url: impl Into<String>,
        key_prefix: impl Into<String>,
        timeout: Duration,
        capacity: usize,
        batch_size: usize,
    ) -> BpResult<Self> {
        let config = Self {
            valkey: ValkeyReplayStoreConfig::new(url, key_prefix, timeout)?,
            capacity,
            batch_size,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn from_url(url: impl Into<String>, timeout: Duration) -> BpResult<Self> {
        Self::new(
            url,
            DEFAULT_TOKEN_USAGE_QUEUE_PREFIX,
            timeout,
            DEFAULT_TOKEN_USAGE_QUEUE_CAPACITY,
            DEFAULT_TOKEN_USAGE_QUEUE_BATCH_SIZE,
        )
    }

    pub fn url(&self) -> &str {
        self.valkey.url()
    }

    pub fn key_prefix(&self) -> &str {
        self.valkey.key_prefix()
    }

    pub fn timeout(&self) -> Duration {
        self.valkey.timeout()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn validate(&self) -> BpResult<()> {
        self.valkey.validate()?;
        if self.capacity == 0 {
            return Err(BpError::config_invalid(
                "token_usage_queue_capacity",
                "must be greater than zero",
            ));
        }
        if self.batch_size == 0 {
            return Err(BpError::config_invalid(
                "token_usage_queue_batch_size",
                "must be greater than zero",
            ));
        }
        if self.batch_size > self.capacity {
            return Err(BpError::config_invalid(
                "token_usage_queue_batch_size",
                "must not exceed the queue capacity",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for TokenUsageQueueConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenUsageQueueConfig")
            .field("valkey", &self.valkey)
            .field("capacity", &self.capacity)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}
