use super::{
    CwasmLimits, DEFAULT_CWASM_DOWNLOAD_TIMEOUT, DEFAULT_EXECUTION_TIMEOUT,
    DEFAULT_MAX_CWASM_BYTES, DEFAULT_MAX_GUEST_MEMORY_BYTES, DEFAULT_MAX_TABLE_ELEMENTS,
    ExecutionLimits,
};
use platform_errors::{BpError, BpResult};
use std::{env::VarError, num::NonZeroU64, time::Duration};

const EXECUTION_TIMEOUT_SECS: &str = "CWASM_INVOKER_EXECUTION_TIMEOUT_SECS";
const MAX_GUEST_MEMORY_BYTES: &str = "CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES";
const MAX_TABLE_ELEMENTS: &str = "CWASM_INVOKER_MAX_TABLE_ELEMENTS";
const CWASM_DOWNLOAD_TIMEOUT_SECS: &str = "CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS";
const MAX_CWASM_BYTES: &str = "CWASM_INVOKER_MAX_CWASM_BYTES";

/// Runtime and download limits for the native cwasm host.
///
/// Limits are optional environment variables with the same defaults as the
/// compile-time constants, so existing deployments keep their behaviour while
/// operators can tune the sandbox per environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InvokerConfig {
    pub execution: ExecutionLimits,
    pub cwasm: CwasmLimits,
}

impl InvokerConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_lookup(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(VarError::NotPresent) => Ok(None),
            Err(VarError::NotUnicode(_)) => Err(BpError::config_not_unicode(name)),
        })
    }

    pub fn from_lookup<F>(mut lookup: F) -> BpResult<Self>
    where
        F: FnMut(&str) -> BpResult<Option<String>>,
    {
        let execution_timeout = Self::nonzero_u64(
            &mut lookup,
            EXECUTION_TIMEOUT_SECS,
            DEFAULT_EXECUTION_TIMEOUT.as_secs(),
        )?;
        let max_memory_bytes = Self::nonzero_usize(
            &mut lookup,
            MAX_GUEST_MEMORY_BYTES,
            DEFAULT_MAX_GUEST_MEMORY_BYTES,
        )?;
        let max_table_elements =
            Self::nonzero_usize(&mut lookup, MAX_TABLE_ELEMENTS, DEFAULT_MAX_TABLE_ELEMENTS)?;
        let download_timeout = Self::nonzero_u64(
            &mut lookup,
            CWASM_DOWNLOAD_TIMEOUT_SECS,
            DEFAULT_CWASM_DOWNLOAD_TIMEOUT.as_secs(),
        )?;
        let max_cwasm_bytes =
            Self::nonzero_u64(&mut lookup, MAX_CWASM_BYTES, DEFAULT_MAX_CWASM_BYTES.get())?;

        Ok(Self {
            execution: ExecutionLimits::with_table_elements(
                Duration::from_secs(execution_timeout),
                max_memory_bytes,
                max_table_elements,
            ),
            cwasm: CwasmLimits::new(
                Duration::from_secs(download_timeout),
                NonZeroU64::new(max_cwasm_bytes).expect("cwasm limit is validated as non-zero"),
            ),
        })
    }

    fn read<F>(lookup: &mut F, name: &str) -> BpResult<Option<(String, String)>>
    where
        F: FnMut(&str) -> BpResult<Option<String>>,
    {
        match lookup(name)? {
            Some(value) if !value.is_empty() => Ok(Some((name.to_string(), value))),
            _ => Ok(None),
        }
    }

    fn nonzero_u64<F>(lookup: &mut F, name: &str, default: u64) -> BpResult<u64>
    where
        F: FnMut(&str) -> BpResult<Option<String>>,
    {
        let Some((name, value)) = Self::read(lookup, name)? else {
            return Ok(default);
        };
        let value = value
            .parse::<u64>()
            .map_err(|error| BpError::config_invalid_source(name.clone(), error))?;
        if value == 0 {
            return Err(BpError::config_invalid(name, "must be greater than zero"));
        }
        Ok(value)
    }

    fn nonzero_usize<F>(lookup: &mut F, name: &str, default: usize) -> BpResult<usize>
    where
        F: FnMut(&str) -> BpResult<Option<String>>,
    {
        let Some((name, value)) = Self::read(lookup, name)? else {
            return Ok(default);
        };
        let value = value
            .parse::<usize>()
            .map_err(|error| BpError::config_invalid_source(name.clone(), error))?;
        if value == 0 {
            return Err(BpError::config_invalid(name, "must be greater than zero"));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(vars: &[(&str, &str)]) -> BpResult<InvokerConfig> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect();
        InvokerConfig::from_lookup(|name| Ok(vars.get(name).cloned()))
    }

    #[test]
    fn defaults_match_the_compile_time_limits() {
        let config = config(&[]).unwrap();

        assert_eq!(config, InvokerConfig::default());
        assert_eq!(config.execution.timeout, DEFAULT_EXECUTION_TIMEOUT);
        assert_eq!(
            config.execution.max_memory_bytes,
            DEFAULT_MAX_GUEST_MEMORY_BYTES
        );
        assert_eq!(
            config.execution.max_table_elements,
            DEFAULT_MAX_TABLE_ELEMENTS
        );
        assert_eq!(
            config.cwasm.download_timeout,
            DEFAULT_CWASM_DOWNLOAD_TIMEOUT
        );
        assert_eq!(config.cwasm.max_bytes, DEFAULT_MAX_CWASM_BYTES);
    }

    #[test]
    fn reads_overrides_from_the_environment() {
        let config = config(&[
            ("CWASM_INVOKER_EXECUTION_TIMEOUT_SECS", "45"),
            ("CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES", "1048576"),
            ("CWASM_INVOKER_MAX_TABLE_ELEMENTS", "2048"),
            ("CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS", "15"),
            ("CWASM_INVOKER_MAX_CWASM_BYTES", "4096"),
        ])
        .unwrap();

        assert_eq!(config.execution.timeout, Duration::from_secs(45));
        assert_eq!(config.execution.max_memory_bytes, 1024 * 1024);
        assert_eq!(config.execution.max_table_elements, 2048);
        assert_eq!(config.cwasm.download_timeout, Duration::from_secs(15));
        assert_eq!(config.cwasm.max_bytes.get(), 4096);
    }

    #[test]
    fn treats_empty_values_as_unset() {
        let config = config(&[
            ("CWASM_INVOKER_EXECUTION_TIMEOUT_SECS", ""),
            ("CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES", ""),
        ])
        .unwrap();

        assert_eq!(config, InvokerConfig::default());
    }

    #[test]
    fn rejects_zero_and_invalid_values() {
        for (name, value) in [
            ("CWASM_INVOKER_EXECUTION_TIMEOUT_SECS", "0"),
            ("CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES", "0"),
            ("CWASM_INVOKER_MAX_TABLE_ELEMENTS", "0"),
            ("CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS", "0"),
            ("CWASM_INVOKER_MAX_CWASM_BYTES", "0"),
            ("CWASM_INVOKER_MAX_CWASM_BYTES", "not-a-number"),
        ] {
            assert!(config(&[(name, value)]).is_err(), "{name}={value}");
        }
    }
}
