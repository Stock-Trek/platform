use crate::StreamSpec;
use crate::jetstream::stream_spec::{DEFAULT_DUPLICATE_WINDOW, DEFAULT_STREAM_REPLICAS};
use platform_config::EnvVars;
use platform_errors::PlatformResult;
use std::time::Duration;

const REPLICAS_SUFFIX: &str = "REPLICAS";
const MAX_AGE_SUFFIX: &str = "MAX_AGE_SECS";
const MAX_BYTES_SUFFIX: &str = "MAX_BYTES";
const MAX_MESSAGES_SUFFIX: &str = "MAX_MESSAGES";
const DUPLICATE_WINDOW_SUFFIX: &str = "DUPLICATE_WINDOW_SECS";

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

    pub const fn max_age(&self) -> Duration {
        self.max_age
    }

    pub const fn max_bytes(&self) -> i64 {
        self.max_bytes
    }

    pub const fn max_messages(&self) -> i64 {
        self.max_messages
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
    pub fn from_vars(
        vars: &EnvVars,
        prefix: &str,
        defaults: StreamDefaults,
    ) -> PlatformResult<Self> {
        let config = Self {
            replicas: vars
                .optional_parsed::<usize>(&Self::name(prefix, REPLICAS_SUFFIX))?
                .unwrap_or(DEFAULT_STREAM_REPLICAS),
            max_age: Duration::from_secs(
                vars.optional_parsed::<u64>(&Self::name(prefix, MAX_AGE_SUFFIX))?
                    .unwrap_or(defaults.max_age.as_secs()),
            ),
            max_bytes: vars
                .optional_parsed::<i64>(&Self::name(prefix, MAX_BYTES_SUFFIX))?
                .unwrap_or(defaults.max_bytes),
            max_messages: vars
                .optional_parsed::<i64>(&Self::name(prefix, MAX_MESSAGES_SUFFIX))?
                .unwrap_or(defaults.max_messages),
            duplicate_window: Duration::from_secs(
                vars.optional_parsed::<u64>(&Self::name(prefix, DUPLICATE_WINDOW_SUFFIX))?
                    .unwrap_or(DEFAULT_DUPLICATE_WINDOW.as_secs()),
            ),
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
