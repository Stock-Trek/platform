use crate::{TokenUsageReservation, ValkeyReplayStoreConfig};
use platform_errors::{PlatformError, PlatformResult};
use redis::{ErrorKind, RedisError, Value};
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
    ) -> PlatformResult<Self> {
        let config = Self {
            valkey: ValkeyReplayStoreConfig::new(url, key_prefix, timeout)?,
            capacity,
            batch_size,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn from_url(url: impl Into<String>, timeout: Duration) -> PlatformResult<Self> {
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

    pub fn validate(&self) -> PlatformResult<()> {
        self.valkey.validate()?;
        if self.capacity == 0 {
            return Err(PlatformError::config_invalid(
                "token_usage_queue_capacity",
                "must be greater than zero",
            ));
        }
        if self.batch_size == 0 {
            return Err(PlatformError::config_invalid(
                "token_usage_queue_batch_size",
                "must be greater than zero",
            ));
        }
        if self.batch_size > self.capacity {
            return Err(PlatformError::config_invalid(
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
const PAYLOAD_FIELD: &str = "payload";
const TARGET: &str = "token-usage-queue";

/// Parses Redis stream replies and maps client failures into platform errors.
pub struct ValkeyTokenUsageReply;

impl ValkeyTokenUsageReply {
    pub fn parse(reply: Value) -> PlatformResult<Vec<TokenUsageReservation>> {
        let Value::Array(streams) = reply else {
            return Ok(Vec::new());
        };
        let mut reservations = Vec::new();
        for stream in streams {
            let Value::Array(parts) = stream else {
                return Err(Self::malformed("stream entry is not an array"));
            };
            let Some(Value::Array(entries)) = parts.get(1) else {
                return Err(Self::malformed("stream entry is missing records"));
            };
            for entry in entries {
                let Value::Array(fields) = entry else {
                    return Err(Self::malformed("stream record is not an array"));
                };
                let id = fields
                    .first()
                    .and_then(Self::string_value)
                    .ok_or_else(|| Self::malformed("stream record is missing an id"))?;
                let payload = Self::payload(fields.get(1))?;
                let usage = serde_json::from_str(&payload)
                    .map_err(|error| Self::decode_error(error.to_string()))?;
                reservations.push(TokenUsageReservation::new(id, usage));
            }
        }
        Ok(reservations)
    }

    pub fn error(operation: &'static str, error: RedisError) -> PlatformError {
        let retryable = error.is_io_error()
            || error.is_timeout()
            || error.is_connection_dropped()
            || error.is_cluster_error()
            || matches!(
                error.kind(),
                ErrorKind::BusyLoadingError
                    | ErrorKind::TryAgain
                    | ErrorKind::ClusterDown
                    | ErrorKind::MasterDown
                    | ErrorKind::ReadOnly
            );
        PlatformError::auth_store(operation, TARGET, retryable, error)
    }

    pub fn malformed(detail: &'static str) -> PlatformError {
        PlatformError::auth_store("reserve", TARGET, false, detail.to_string())
    }

    pub fn decode_error(detail: String) -> PlatformError {
        PlatformError::auth_store("reserve", TARGET, false, detail)
    }

    fn payload(fields: Option<&Value>) -> PlatformResult<String> {
        let Some(Value::Array(fields)) = fields else {
            return Err(Self::malformed("stream record is missing fields"));
        };
        for pair in fields.chunks(2) {
            let [name, value] = pair else {
                continue;
            };
            if Self::string_value(name).as_deref() == Some(PAYLOAD_FIELD) {
                return Self::string_value(value)
                    .ok_or_else(|| Self::malformed("stream payload is not a string"));
            }
        }
        Err(Self::malformed("stream record is missing its payload"))
    }

    fn string_value(value: &Value) -> Option<String> {
        match value {
            Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
            Value::SimpleString(text) => Some(text.clone()),
            Value::VerbatimString { text, .. } => Some(text.clone()),
            _ => None,
        }
    }
}
