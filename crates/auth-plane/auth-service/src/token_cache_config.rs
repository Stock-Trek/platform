use platform_config::env_vars::EnvVars;
use platform_errors::{BuildError, BuildResult};
use std::time::Duration;

const DEFAULT_MAX_ENTRIES: usize = 10_000;
const DEFAULT_NEGATIVE_ENTRIES: usize = 1_000;
const DEFAULT_FRESHNESS: Duration = Duration::from_secs(30);
const DEFAULT_MAX_STALE_AGE: Duration = Duration::from_secs(60);
const DEFAULT_FAILURE_THRESHOLD: usize = 5;
const DEFAULT_OPEN_DURATION: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenCacheConfig {
    max_entries: usize,
    negative_entries: usize,
    freshness: Duration,
    max_stale_age: Duration,
    failure_threshold: usize,
    open_duration: Duration,
}

impl TokenCacheConfig {
    pub fn from_vars(vars: &EnvVars, names: [&str; 6]) -> BuildResult<Self> {
        let defaults = Self::default();
        Self::new(
            vars.optional_parsed(names[0])?
                .unwrap_or(defaults.max_entries()),
            vars.optional_parsed(names[1])?
                .unwrap_or(defaults.negative_entries()),
            Duration::from_secs(
                vars.optional_parsed(names[2])?
                    .unwrap_or(defaults.freshness().as_secs()),
            ),
            Duration::from_secs(
                vars.optional_parsed(names[3])?
                    .unwrap_or(defaults.max_stale_age().as_secs()),
            ),
            vars.optional_parsed(names[4])?
                .unwrap_or(defaults.failure_threshold()),
            Duration::from_secs(
                vars.optional_parsed(names[5])?
                    .unwrap_or(defaults.open_duration().as_secs()),
            ),
        )
    }

    pub fn new(
        max_entries: usize,
        negative_entries: usize,
        freshness: Duration,
        max_stale_age: Duration,
        failure_threshold: usize,
        open_duration: Duration,
    ) -> BuildResult<Self> {
        if max_entries == 0 {
            return Err(BuildError::auth_invalid_field(
                "token_cache_max_entries",
                "must be greater than zero",
            ));
        }
        if negative_entries == 0 {
            return Err(BuildError::auth_invalid_field(
                "token_cache_negative_entries",
                "must be greater than zero",
            ));
        }
        if freshness.is_zero() {
            return Err(BuildError::auth_invalid_field(
                "token_cache_freshness",
                "must be greater than zero",
            ));
        }
        if max_stale_age < freshness {
            return Err(BuildError::auth_invalid_field(
                "token_cache_max_stale_age",
                "must not be shorter than the freshness window",
            ));
        }
        if failure_threshold == 0 {
            return Err(BuildError::auth_invalid_field(
                "token_cache_failure_threshold",
                "must be greater than zero",
            ));
        }
        if open_duration.is_zero() {
            return Err(BuildError::auth_invalid_field(
                "token_cache_open_duration",
                "must be greater than zero",
            ));
        }
        Ok(Self {
            max_entries,
            negative_entries,
            freshness,
            max_stale_age,
            failure_threshold,
            open_duration,
        })
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub fn negative_entries(&self) -> usize {
        self.negative_entries
    }

    pub fn freshness(&self) -> Duration {
        self.freshness
    }

    pub fn max_stale_age(&self) -> Duration {
        self.max_stale_age
    }

    pub fn failure_threshold(&self) -> usize {
        self.failure_threshold
    }

    pub fn open_duration(&self) -> Duration {
        self.open_duration
    }
}

impl Default for TokenCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_ENTRIES,
            negative_entries: DEFAULT_NEGATIVE_ENTRIES,
            freshness: DEFAULT_FRESHNESS,
            max_stale_age: DEFAULT_MAX_STALE_AGE,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            open_duration: DEFAULT_OPEN_DURATION,
        }
    }
}
