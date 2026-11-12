use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_nats::StreamSpec;
use std::time::Duration;

const REPLICAS_SUFFIX: &str = "REPLICAS";
const MAX_AGE_SUFFIX: &str = "MAX_AGE_SECS";
const MAX_BYTES_SUFFIX: &str = "MAX_BYTES";
const MAX_MESSAGES_SUFFIX: &str = "MAX_MESSAGES";
const DUPLICATE_WINDOW_SUFFIX: &str = "DUPLICATE_WINDOW_SECS";
const DEFAULT_REPLICAS: usize = 3;
const DEFAULT_DUPLICATE_WINDOW_SECS: u64 = 120;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDefaults {
    max_age: Duration,
    max_bytes: i64,
    max_messages: i64,
}

impl StreamDefaults {
    pub const fn new(max_age: Duration, max_bytes: i64, max_messages: i64) -> Self {
        Self {
            max_age,
            max_bytes,
            max_messages,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NatsStreamConfig {
    replicas: usize,
    max_age: Duration,
    max_bytes: i64,
    max_messages: i64,
    duplicate_window: Duration,
}

impl NatsStreamConfig {
    pub fn from_vars(vars: &EnvVars, prefix: &str, defaults: StreamDefaults) -> BpResult<Self> {
        let replicas = vars
            .optional_parsed::<usize>(&Self::name(prefix, REPLICAS_SUFFIX))?
            .unwrap_or(DEFAULT_REPLICAS);
        let max_age = Duration::from_secs(
            vars.optional_parsed::<u64>(&Self::name(prefix, MAX_AGE_SUFFIX))?
                .unwrap_or(defaults.max_age.as_secs()),
        );
        let max_bytes = vars
            .optional_parsed::<i64>(&Self::name(prefix, MAX_BYTES_SUFFIX))?
            .unwrap_or(defaults.max_bytes);
        let max_messages = vars
            .optional_parsed::<i64>(&Self::name(prefix, MAX_MESSAGES_SUFFIX))?
            .unwrap_or(defaults.max_messages);
        let duplicate_window = Duration::from_secs(
            vars.optional_parsed::<u64>(&Self::name(prefix, DUPLICATE_WINDOW_SUFFIX))?
                .unwrap_or(DEFAULT_DUPLICATE_WINDOW_SECS),
        );
        let config = Self {
            replicas,
            max_age,
            max_bytes,
            max_messages,
            duplicate_window,
        };
        config.spec("portfolio").validate()?;
        Ok(config)
    }

    pub fn spec(&self, subject_prefix: &str) -> StreamSpec {
        StreamSpec::new(
            subject_prefix,
            self.max_age,
            self.max_bytes,
            self.max_messages,
        )
        .with_replicas(self.replicas)
        .with_duplicate_window(self.duplicate_window)
    }

    pub fn replicas(&self) -> usize {
        self.replicas
    }

    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    pub fn max_bytes(&self) -> i64 {
        self.max_bytes
    }

    pub fn max_messages(&self) -> i64 {
        self.max_messages
    }

    pub fn duplicate_window(&self) -> Duration {
        self.duplicate_window
    }

    fn name(prefix: &str, suffix: &str) -> String {
        format!("{prefix}_{suffix}")
    }
}
