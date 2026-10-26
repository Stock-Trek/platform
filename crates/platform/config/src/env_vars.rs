//! Helpers for reading required environment variables.
//!
//! Environment variables are parsed by each process's own configuration type.
//! This module only provides the raw, fallible reads; it deliberately has no
//! knowledge of which variables any particular process needs.

use platform_errors::{BpError, BpResult};
use chrono::{DateTime, NaiveDate, Utc};
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

    /// Reads a required duration expressed as whole seconds.
    pub fn required_duration_secs(&self, name: &str) -> BpResult<Duration> {
        Ok(Duration::from_secs(self.required_parsed(name)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use platform_errors::ConfigError;

    fn env_vars<I, K, V>(values: I) -> EnvVars
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<OsString>,
        V: Into<OsString>,
    {
        EnvVars::from_map(values)
    }

    #[test]
    fn required_reads_a_value() {
        let vars = env_vars([("NAME", "value")]);
        assert_eq!(vars.required("NAME").unwrap(), "value");
    }

    #[test]
    fn required_reports_missing_and_empty_values() {
        let vars = env_vars(Vec::<(String, String)>::new());
        assert!(matches!(
            vars.required("MISSING").unwrap_err(),
            BpError::Config(ConfigError::Missing { name }) if name == "MISSING"
        ));

        let vars = env_vars([("EMPTY", "")]);
        assert!(matches!(
            vars.required("EMPTY").unwrap_err(),
            BpError::Config(ConfigError::Empty { name }) if name == "EMPTY"
        ));
    }

    #[test]
    fn required_parsed_reports_invalid_values() {
        let vars = env_vars([("COUNT", "not a number")]);
        assert!(matches!(
            vars.required_parsed::<usize>("COUNT").unwrap_err(),
            BpError::Config(ConfigError::Invalid { name, .. }) if name == "COUNT"
        ));
    }

    #[test]
    fn required_nonzero_rejects_zero() {
        let vars = env_vars([("COUNT", "0")]);
        assert!(vars.required_nonzero_usize("COUNT").is_err());
        assert!(vars.required_nonzero_u64("COUNT").is_err());
    }

    #[test]
    fn required_duration_uses_seconds() {
        let vars = env_vars([("DURATION_SECS", "42")]);
        assert_eq!(
            vars.required_duration_secs("DURATION_SECS").unwrap(),
            Duration::from_secs(42)
        );
    }

    #[test]
    fn required_url_validates_the_scheme() {
        let vars = env_vars([("URL", "wss://example.test/ws")]);
        assert_eq!(
            vars.required_url("URL", &["ws", "wss"]).unwrap(),
            "wss://example.test/ws"
        );
        assert!(vars.required_url("URL", &["http", "https"]).is_err());
    }

    #[test]
    fn required_timestamp_accepts_rfc3339_and_dates() {
        let vars = env_vars([("START", "2024-01-02")]);
        assert_eq!(
            vars.required_timestamp("START").unwrap().to_rfc3339(),
            "2024-01-02T00:00:00+00:00"
        );

        let vars = env_vars([("START", "2024-01-02T03:04:05Z")]);
        assert_eq!(
            vars.required_timestamp("START").unwrap().to_rfc3339(),
            "2024-01-02T03:04:05+00:00"
        );
    }

    #[test]
    fn required_log_filter_rejects_invalid_directives() {
        let vars = env_vars([("RUST_LOG", "not a valid [directive")]);
        assert!(vars.required_log_filter("RUST_LOG").is_err());
    }

    #[test]
    #[cfg(unix)]
    fn required_reports_not_unicode_without_exposing_the_value() {
        use std::os::unix::ffi::OsStringExt;

        let vars = env_vars([(
            OsString::from("SECRET"),
            OsString::from_vec(b"super-secret-\xff".to_vec()),
        )]);
        let error = vars.required("SECRET").unwrap_err();

        assert!(matches!(
            &error,
            BpError::Config(ConfigError::NotUnicode { name }) if name == "SECRET"
        ));
        assert!(!error.to_string().contains("super-secret"));
    }
}
