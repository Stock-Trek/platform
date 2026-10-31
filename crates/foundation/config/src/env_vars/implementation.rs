use chrono::{DateTime, NaiveDate, Utc};
use platform_errors::{BpError, BpResult};
use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};
use tracing_subscriber::EnvFilter;
use url::Url;

/// A snapshot of an environment used while building a process configuration.
///
/// Taking a snapshot up front makes configuration construction deterministic and
/// testable. Every accessor is required: there are no defaults or fallbacks.
#[derive(Debug, Clone, Default)]
pub struct EnvVars {
    vars: HashMap<OsString, OsString>,
}

impl EnvVars {
    /// Captures the process environment.
    pub fn from_env() -> Self {
        Self::from_map(std::env::vars_os())
    }

    /// Builds a snapshot from an explicit set of variables.
    pub fn from_map<I, K, V>(vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<OsString>,
        V: Into<OsString>,
    {
        Self {
            vars: vars
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }

    /// Reads a required UTF-8 variable.
    pub fn required(&self, name: &str) -> BpResult<String> {
        let value = self
            .vars
            .get(OsStr::new(name))
            .ok_or_else(|| BpError::config_missing(name))?;
        let value = value
            .clone()
            .into_string()
            .map_err(|_| BpError::config_not_unicode(name))?;
        if value.is_empty() {
            return Err(BpError::config_empty(name));
        }
        Ok(value)
    }

    /// Reads a required variable even when its value is not valid UTF-8.
    ///
    /// This is only useful for diagnostics: the value is converted lossily and
    /// must not be used as configuration.
    pub fn required_lossy(&self, name: &str) -> BpResult<String> {
        let value = self
            .vars
            .get(OsStr::new(name))
            .ok_or_else(|| BpError::config_missing(name))?;
        if value.is_empty() {
            return Err(BpError::config_empty(name));
        }
        Ok(value.to_string_lossy().into_owned())
    }

    /// Reads an optional UTF-8 variable. Missing and empty values yield `None`.
    pub fn optional(&self, name: &str) -> BpResult<Option<String>> {
        let Some(value) = self.vars.get(OsStr::new(name)) else {
            return Ok(None);
        };
        let value = value
            .clone()
            .into_string()
            .map_err(|_| BpError::config_not_unicode(name))?;
        Ok((!value.is_empty()).then_some(value))
    }

    /// Reads and parses a required variable.
    pub fn required_parsed<T>(&self, name: &str) -> BpResult<T>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        self.required(name)?
            .parse::<T>()
            .map_err(|error| BpError::config_invalid_source(name, error))
    }

    /// Reads and parses an optional variable.
    pub fn optional_parsed<T>(&self, name: &str) -> BpResult<Option<T>>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        match self.optional(name)? {
            Some(value) => value
                .parse::<T>()
                .map(Some)
                .map_err(|error| BpError::config_invalid_source(name, error)),
            None => Ok(None),
        }
    }

    /// Reads and parses an optional variable, falling back to `default` when it
    /// is missing or empty.
    pub fn optional_parsed_or<T>(&self, name: &str, default: T) -> BpResult<T>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        Ok(self.optional_parsed(name)?.unwrap_or(default))
    }

    /// Reads a required duration expressed as whole seconds.
    pub fn required_duration_secs(&self, name: &str) -> BpResult<Duration> {
        Ok(Duration::from_secs(self.required_parsed(name)?))
    }

    /// Reads a required duration expressed as a positive number of seconds.
    pub fn required_positive_duration_secs(&self, name: &str) -> BpResult<Duration> {
        let secs = self.required_parsed(name)?;
        Self::positive_duration_secs(name, secs)
    }

    /// Reads an optional duration expressed as a positive number of seconds,
    /// falling back to `default_secs` when the variable is missing or empty.
    pub fn optional_positive_duration_secs(
        &self,
        name: &str,
        default_secs: u64,
    ) -> BpResult<Duration> {
        let secs = self.optional_parsed_or(name, default_secs)?;
        Self::positive_duration_secs(name, secs)
    }

    /// Reads a required, non-zero `usize`.
    pub fn required_nonzero_usize(&self, name: &str) -> BpResult<NonZeroUsize> {
        let value: usize = self.required_parsed(name)?;
        NonZeroUsize::new(value)
            .ok_or_else(|| BpError::config_invalid(name, "must be greater than zero"))
    }

    /// Reads a required, non-zero `u64`.
    pub fn required_nonzero_u64(&self, name: &str) -> BpResult<NonZeroU64> {
        let value: u64 = self.required_parsed(name)?;
        NonZeroU64::new(value)
            .ok_or_else(|| BpError::config_invalid(name, "must be greater than zero"))
    }

    /// Reads a required path.
    pub fn required_path(&self, name: &str) -> BpResult<PathBuf> {
        Ok(PathBuf::from(self.required(name)?))
    }

    /// Reads a required comma-separated list.
    pub fn required_csv(&self, name: &str) -> BpResult<Vec<String>> {
        Ok(Self::split_csv(&self.required(name)?))
    }

    /// Reads a required RFC 3339 timestamp or `YYYY-MM-DD` date.
    pub fn required_timestamp(&self, name: &str) -> BpResult<DateTime<Utc>> {
        let value = self.required(name)?;
        Self::parse_timestamp(name, value.trim())
    }

    /// Reads a required URL and validates its scheme.
    pub fn required_url(&self, name: &str, allowed_schemes: &[&str]) -> BpResult<String> {
        let value = self.required(name)?;
        let url =
            Url::parse(&value).map_err(|error| BpError::config_invalid_source(name, error))?;
        if !allowed_schemes.contains(&url.scheme()) {
            return Err(BpError::config_invalid(
                name,
                format!(
                    "must use one of the schemes {}, got \"{}\"",
                    allowed_schemes.join(", "),
                    url.scheme()
                ),
            ));
        }
        Ok(value)
    }

    /// Reads and validates a required tracing filter.
    pub fn required_log_filter(&self, name: &str) -> BpResult<String> {
        let value = self.required(name)?;
        EnvFilter::try_new(&value).map_err(|error| BpError::config_invalid_source(name, error))?;
        Ok(value)
    }

    fn positive_duration_secs(name: &str, secs: u64) -> BpResult<Duration> {
        let secs = NonZeroU64::new(secs)
            .ok_or_else(|| BpError::config_invalid(name, "must be greater than zero"))?;
        Ok(Duration::from_secs(secs.get()))
    }

    fn parse_timestamp(name: &str, value: &str) -> BpResult<DateTime<Utc>> {
        if let Ok(date_time) = DateTime::parse_from_rfc3339(value) {
            return Ok(date_time.with_timezone(&Utc));
        }
        if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            let Some(date_time) = date.and_hms_opt(0, 0, 0) else {
                return Err(Self::invalid_timestamp(name, value));
            };
            return Ok(date_time.and_utc());
        }
        Err(Self::invalid_timestamp(name, value))
    }

    fn invalid_timestamp(name: &str, value: &str) -> BpError {
        BpError::config_invalid(
            name,
            format!("must be an RFC 3339 timestamp or YYYY-MM-DD date, got {value}"),
        )
    }

    fn split_csv(value: &str) -> Vec<String> {
        value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect()
    }
}
