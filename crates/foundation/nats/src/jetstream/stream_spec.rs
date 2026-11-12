use crate::jetstream::JetStream;
use async_nats::jetstream::stream;
use platform_errors::{BuildError, BuildResult};
use std::time::Duration;

pub const MIN_STREAM_REPLICAS: usize = 1;
pub const MAX_STREAM_REPLICAS: usize = 5;
pub const DEFAULT_STREAM_REPLICAS: usize = 3;
pub const DEFAULT_DUPLICATE_WINDOW: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSpec {
    subject_prefix: String,
    replicas: usize,
    max_age: Duration,
    max_bytes: i64,
    max_messages: i64,
    duplicate_window: Duration,
}

impl StreamSpec {
    pub fn new(
        subject_prefix: impl Into<String>,
        max_age: Duration,
        max_bytes: i64,
        max_messages: i64,
    ) -> Self {
        Self {
            subject_prefix: subject_prefix.into(),
            replicas: DEFAULT_STREAM_REPLICAS,
            max_age,
            max_bytes,
            max_messages,
            duplicate_window: DEFAULT_DUPLICATE_WINDOW,
        }
    }

    pub fn with_replicas(mut self, replicas: usize) -> Self {
        self.replicas = replicas;
        self
    }

    pub fn with_duplicate_window(mut self, duplicate_window: Duration) -> Self {
        self.duplicate_window = duplicate_window;
        self
    }

    pub fn subject_prefix(&self) -> &str {
        &self.subject_prefix
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

    pub fn validate(&self) -> BuildResult<()> {
        if self.subject_prefix.trim().is_empty() {
            return Err(BuildError::config_invalid(
                "stream subject prefix",
                "must not be empty",
            ));
        }
        if !(MIN_STREAM_REPLICAS..=MAX_STREAM_REPLICAS).contains(&self.replicas) {
            return Err(BuildError::config_invalid(
                "stream replicas",
                format!("must be between {MIN_STREAM_REPLICAS} and {MAX_STREAM_REPLICAS}"),
            ));
        }
        if self.max_age.is_zero() {
            return Err(BuildError::config_invalid(
                "stream max age",
                "must be greater than zero",
            ));
        }
        if self.max_bytes <= 0 {
            return Err(BuildError::config_invalid(
                "stream max bytes",
                "must be greater than zero",
            ));
        }
        if self.max_messages <= 0 {
            return Err(BuildError::config_invalid(
                "stream max messages",
                "must be greater than zero",
            ));
        }
        if self.duplicate_window.is_zero() || self.duplicate_window > self.max_age {
            return Err(BuildError::config_invalid(
                "stream duplicate window",
                "must be greater than zero and no greater than the max age",
            ));
        }
        Ok(())
    }

    pub(crate) fn config(&self) -> BuildResult<stream::Config> {
        self.validate()?;
        Ok(stream::Config {
            name: JetStream::stream_name(&self.subject_prefix),
            subjects: vec![format!("{}.>", self.subject_prefix)],
            storage: stream::StorageType::File,
            retention: stream::RetentionPolicy::Limits,
            max_age: self.max_age,
            max_bytes: self.max_bytes,
            max_messages: self.max_messages,
            num_replicas: self.replicas,
            duplicate_window: self.duplicate_window,
            ..Default::default()
        })
    }
}

impl TryFrom<&StreamSpec> for stream::Config {
    type Error = BuildError;

    fn try_from(spec: &StreamSpec) -> BuildResult<Self> {
        spec.config()
    }
}
