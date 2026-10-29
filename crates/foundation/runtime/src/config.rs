//! Configuration helpers shared by services across the workspace.
//!
//! Service-specific configuration types still describe their own environment
//! variables, but the common validation rules live here so every service
//! rejects the same malformed values in the same way.

use platform_config::env_names::RUST_LOG;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_nats::{NatsAddress, NatsSubject};
use std::{num::NonZeroUsize, time::Duration};
use stock_trek_types::cex::cex_id::CexId;

/// Common configuration validation and parsing helpers.
pub struct Config;

impl Config {
    pub const RUST_LOG: &str = RUST_LOG;

    /// Reads and validates the common tracing filter.
    pub fn log_filter(vars: &EnvVars) -> BpResult<String> {
        vars.required_log_filter(Self::RUST_LOG)
    }

    /// Reads a required NATS URL and validates its scheme and address.
    pub fn required_nats_url(vars: &EnvVars, name: &str) -> BpResult<String> {
        let value = vars.required(name)?;
        NatsAddress::validate_named(name, &value)?;
        Ok(value)
    }

    /// Reads a required NATS subject prefix and validates it.
    pub fn required_nats_subject(vars: &EnvVars, name: &str) -> BpResult<String> {
        let value = vars.required(name)?;
        NatsSubject::validate_named(name, &value)
            .map_err(|error| BpError::config_invalid(name, error.to_string()))?;
        Ok(value)
    }

    /// Reads a required exchange identifier and converts it to a [`CexId`].
    pub fn required_exchange(vars: &EnvVars, name: &str) -> BpResult<CexId> {
        let value = vars.required(name)?;
        CexId::try_from(value.as_str())
            .map_err(|_| BpError::config_invalid(name, format!("unsupported exchange {value}")))
    }

    /// Reads a required duration expressed as a positive number of seconds.
    pub fn required_positive_duration_secs(vars: &EnvVars, name: &str) -> BpResult<Duration> {
        vars.required_positive_duration_secs(name)
    }

    /// Reads an optional duration expressed as a positive number of seconds,
    /// falling back to `default_secs` when the variable is missing or empty.
    pub fn optional_positive_duration_secs(
        vars: &EnvVars,
        name: &str,
        default_secs: u64,
    ) -> BpResult<Duration> {
        vars.optional_positive_duration_secs(name, default_secs)
    }

    /// Reads a required positive `usize`.
    pub fn required_positive_usize(vars: &EnvVars, name: &str) -> BpResult<usize> {
        let value = vars.required_parsed(name)?;
        Self::positive_usize(name, value)
    }

    /// Reads an optional positive `usize`, falling back to `default` when the
    /// variable is missing or empty.
    pub fn optional_positive_usize(vars: &EnvVars, name: &str, default: usize) -> BpResult<usize> {
        let value = vars.optional_parsed_or(name, default)?;
        Self::positive_usize(name, value)
    }

    fn positive_usize(name: &str, value: usize) -> BpResult<usize> {
        NonZeroUsize::new(value)
            .map(NonZeroUsize::get)
            .ok_or_else(|| BpError::config_invalid(name, "must be greater than zero"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_errors::ConfigError;

    fn vars<I, K, V>(values: I) -> EnvVars
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<std::ffi::OsString>,
        V: Into<std::ffi::OsString>,
    {
        EnvVars::from_map(values)
    }

    #[test]
    fn log_filter_is_required_and_validated() {
        assert_eq!(
            Config::log_filter(&vars([(Config::RUST_LOG, "info")])).unwrap(),
            "info"
        );
        assert!(Config::log_filter(&vars(Vec::<(String, String)>::new())).is_err());
        assert!(Config::log_filter(&vars([(Config::RUST_LOG, "not a valid [directive")])).is_err());
    }

    #[test]
    fn nats_url_must_be_a_valid_nats_address() {
        assert_eq!(
            Config::required_nats_url(
                &vars([("SERVICE_NATS_URL", "nats://localhost:4222")]),
                "SERVICE_NATS_URL"
            )
            .unwrap(),
            "nats://localhost:4222"
        );
        assert!(matches!(
            Config::required_nats_url(
                &vars([("SERVICE_NATS_URL", "http://localhost:4222")]),
                "SERVICE_NATS_URL"
            )
            .unwrap_err(),
            BpError::Config(ConfigError::Invalid { name, .. }) if name == "SERVICE_NATS_URL"
        ));
    }

    #[test]
    fn nats_subjects_reject_wildcards_and_invalid_segments() {
        assert_eq!(
            Config::required_nats_subject(
                &vars([("SERVICE_SUBJECT", "market.events.binance")]),
                "SERVICE_SUBJECT"
            )
            .unwrap(),
            "market.events.binance"
        );
        for subject in ["market.*", "market.>", "market..events"] {
            assert!(matches!(
                Config::required_nats_subject(
                    &vars([("SERVICE_SUBJECT", subject)]),
                    "SERVICE_SUBJECT"
                )
                .unwrap_err(),
                BpError::Config(ConfigError::Invalid { name, .. }) if name == "SERVICE_SUBJECT"
            ));
        }
        assert!(
            Config::required_nats_subject(&vars([("SERVICE_SUBJECT", "")]), "SERVICE_SUBJECT")
                .is_err()
        );
    }

    #[test]
    fn exchanges_must_be_required_and_supported() {
        assert_eq!(
            Config::required_exchange(&vars([("SERVICE_EXCHANGE", "binance")]), "SERVICE_EXCHANGE")
                .unwrap(),
            CexId::Binance
        );
        for value in ["kraken", "not an exchange"] {
            assert!(matches!(
                Config::required_exchange(&vars([("SERVICE_EXCHANGE", value)]), "SERVICE_EXCHANGE")
                    .unwrap_err(),
                BpError::Config(ConfigError::Invalid { name, .. }) if name == "SERVICE_EXCHANGE"
            ));
        }
        assert!(
            Config::required_exchange(&vars([("SERVICE_EXCHANGE", "")]), "SERVICE_EXCHANGE")
                .is_err()
        );
        assert!(
            Config::required_exchange(&vars(Vec::<(String, String)>::new()), "SERVICE_EXCHANGE")
                .is_err()
        );
    }

    #[test]
    fn positive_durations_reject_zero_and_invalid_values() {
        assert_eq!(
            Config::optional_positive_duration_secs(
                &vars([("INTERVAL_SECS", "30")]),
                "INTERVAL_SECS",
                5
            )
            .unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            Config::optional_positive_duration_secs(
                &vars(Vec::<(String, String)>::new()),
                "INTERVAL_SECS",
                5
            )
            .unwrap(),
            Duration::from_secs(5)
        );
        assert!(
            Config::optional_positive_duration_secs(
                &vars([("INTERVAL_SECS", "0")]),
                "INTERVAL_SECS",
                5
            )
            .is_err()
        );
        assert!(
            Config::required_positive_duration_secs(
                &vars([("INTERVAL_SECS", "0")]),
                "INTERVAL_SECS"
            )
            .is_err()
        );
        assert!(
            Config::required_positive_duration_secs(
                &vars([("INTERVAL_SECS", "not a number")]),
                "INTERVAL_SECS"
            )
            .is_err()
        );
    }

    #[test]
    fn positive_usize_helpers_reject_zero() {
        assert_eq!(
            Config::optional_positive_usize(&vars([("COUNT", "7")]), "COUNT", 1).unwrap(),
            7
        );
        assert_eq!(
            Config::optional_positive_usize(&vars(Vec::<(String, String)>::new()), "COUNT", 1)
                .unwrap(),
            1
        );
        assert!(Config::optional_positive_usize(&vars([("COUNT", "0")]), "COUNT", 1).is_err());
        assert!(Config::required_positive_usize(&vars([("COUNT", "0")]), "COUNT").is_err());
    }
}
