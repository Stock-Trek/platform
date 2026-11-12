use super::{NatsStreamConfig, StreamDefaults};
use platform_config::env_vars::EnvVars;
use platform_errors::BuildResult;
use platform_runtime::Config;
use std::time::Duration;

const STREAM_PREFIX: &str = "PORTFOLIO_OWNERSHIP_STREAM";
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const DEFAULT_MAX_BYTES: i64 = 1024 * 1024 * 1024;
const DEFAULT_MAX_MESSAGES: i64 = 10_000_000;

const NATS_URL: &str = "PORTFOLIO_OWNERSHIP_NATS_URL";
const DURABLE: &str = "PORTFOLIO_OWNERSHIP_DURABLE";
const CHECKPOINT_BUCKET: &str = "PORTFOLIO_OWNERSHIP_CHECKPOINT_BUCKET";
const CLIENT_NAME: &str = "PORTFOLIO_OWNERSHIP_CLIENT_NAME";
const RETRY_BACKOFF_SECS: &str = "PORTFOLIO_OWNERSHIP_RETRY_BACKOFF_SECS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipConfig {
    nats_url: String,
    client_name: String,
    durable: String,
    checkpoint_bucket: String,
    retry_backoff: Duration,
    stream: NatsStreamConfig,
}

impl OwnershipConfig {
    pub fn from_vars(vars: &EnvVars) -> BuildResult<Self> {
        Ok(Self {
            nats_url: Config::required_nats_url(vars, NATS_URL)?,
            client_name: vars.required(CLIENT_NAME)?,
            durable: vars.required(DURABLE)?,
            checkpoint_bucket: vars.required(CHECKPOINT_BUCKET)?,
            retry_backoff: vars.required_positive_duration_secs(RETRY_BACKOFF_SECS)?,
            stream: NatsStreamConfig::from_vars(
                vars,
                STREAM_PREFIX,
                StreamDefaults::new(DEFAULT_MAX_AGE, DEFAULT_MAX_BYTES, DEFAULT_MAX_MESSAGES),
            )?,
        })
    }

    pub fn nats_url(&self) -> &str {
        &self.nats_url
    }

    pub fn client_name(&self) -> &str {
        &self.client_name
    }

    pub fn durable(&self) -> &str {
        &self.durable
    }

    pub fn checkpoint_bucket(&self) -> &str {
        &self.checkpoint_bucket
    }

    pub fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }

    pub fn stream(&self) -> NatsStreamConfig {
        self.stream
    }
}
