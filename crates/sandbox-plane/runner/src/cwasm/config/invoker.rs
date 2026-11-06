use platform_errors::{BpError, BpResult};
use std::{env::VarError, num::NonZeroU64, time::Duration};

use crate::cwasm::{
    CwasmLimits, DEFAULT_CWASM_DOWNLOAD_TIMEOUT, DEFAULT_EXECUTION_TIMEOUT,
    DEFAULT_MAX_CWASM_BYTES, DEFAULT_MAX_GUEST_MEMORY_BYTES, DEFAULT_MAX_TABLE_ELEMENTS,
    ExecutionLimits,
};

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
