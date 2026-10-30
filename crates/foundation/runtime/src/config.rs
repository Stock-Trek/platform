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
