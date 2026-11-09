use platform_auth_service::{ValkeyReplayStoreConfig, VaultTokenStoreConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult, ConfigError};
use secrecy::SecretString;
use std::time::Duration;

use crate::cwasm::{CwasmLimits, ExecutionLimits};

const EXECUTION_TIMEOUT_SECS: &str = "CWASM_INVOKER_EXECUTION_TIMEOUT_SECS";
const MAX_GUEST_MEMORY_BYTES: &str = "CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES";
const MAX_TABLE_ELEMENTS: &str = "CWASM_INVOKER_MAX_TABLE_ELEMENTS";
const CWASM_DOWNLOAD_TIMEOUT_SECS: &str = "CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS";
const MAX_CWASM_BYTES: &str = "CWASM_INVOKER_MAX_CWASM_BYTES";
const VAULT_ADDRESS: &str = "CWASM_INVOKER_VAULT_ADDRESS";
const VAULT_TOKEN: &str = "CWASM_INVOKER_VAULT_TOKEN";
const VAULT_MOUNT: &str = "CWASM_INVOKER_VAULT_MOUNT";
const AUDIT_QUEUE_URL: &str = "CWASM_INVOKER_AUDIT_QUEUE_URL";
const AUDIT_TIMEOUT_SECS: &str = "CWASM_INVOKER_AUDIT_TIMEOUT_SECS";
const VALKEY_URL: &str = "CWASM_INVOKER_VALKEY_URL";
const VALKEY_URL_ALIASES: [&str; 2] = [
    "CWASM_INVOKER_VALKEY_ADDRESS",
    "CWASM_INVOKER_REPLAY_VALKEY_URL",
];
const VALKEY_KEY_PREFIX: &str = "CWASM_INVOKER_VALKEY_KEY_PREFIX";
const VALKEY_KEY_PREFIX_ALIASES: [&str; 3] = [
    "CWASM_INVOKER_VALKEY_PREFIX",
    "CWASM_INVOKER_VALKEY_REPLAY_PREFIX",
    "CWASM_INVOKER_REPLAY_PREFIX",
];
const VALKEY_TIMEOUT_SECS: &str = "CWASM_INVOKER_VALKEY_TIMEOUT_SECS";
const VALKEY_TIMEOUT_ALIASES: [&str; 2] = [
    "CWASM_INVOKER_REPLAY_VALKEY_TIMEOUT_SECS",
    "CWASM_INVOKER_REPLAY_TIMEOUT_SECS",
];
const VAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct AuditConfig {
    pub queue_url: String,
    pub timeout: Duration,
}

impl AuditConfig {
    fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            queue_url: vars.required_url(AUDIT_QUEUE_URL, &["http", "https"])?,
            timeout: vars.required_positive_duration_secs(AUDIT_TIMEOUT_SECS)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct InvokerConfig {
    pub execution: ExecutionLimits,
    pub cwasm: CwasmLimits,
    pub vault: VaultTokenStoreConfig,
    pub valkey: ValkeyReplayStoreConfig,
    pub audit: AuditConfig,
}

impl InvokerConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let execution = ExecutionLimits::with_table_elements(
            vars.required_positive_duration_secs(EXECUTION_TIMEOUT_SECS)?,
            vars.required_nonzero_usize(MAX_GUEST_MEMORY_BYTES)?.get(),
            vars.required_nonzero_usize(MAX_TABLE_ELEMENTS)?.get(),
        );
        let cwasm = CwasmLimits::new(
            vars.required_positive_duration_secs(CWASM_DOWNLOAD_TIMEOUT_SECS)?,
            vars.required_nonzero_u64(MAX_CWASM_BYTES)?,
        );
        let vault = VaultTokenStoreConfig::new(
            vars.required_url(VAULT_ADDRESS, &["http", "https"])?,
            SecretString::from(vars.required(VAULT_TOKEN)?),
            vars.required(VAULT_MOUNT)?,
            VAULT_TIMEOUT,
        )?;
        let valkey = ValkeyReplayStoreConfig::new(
            Self::required_any(vars, VALKEY_URL, &VALKEY_URL_ALIASES)?,
            Self::required_any(vars, VALKEY_KEY_PREFIX, &VALKEY_KEY_PREFIX_ALIASES)?,
            Self::required_positive_duration(vars, VALKEY_TIMEOUT_SECS, &VALKEY_TIMEOUT_ALIASES)?,
        )?;
        let audit = AuditConfig::from_vars(vars)?;

        Ok(Self {
            execution,
            cwasm,
            vault,
            valkey,
            audit,
        })
    }

    fn required_any(vars: &EnvVars, name: &str, aliases: &[&str]) -> BpResult<String> {
        match vars.required(name) {
            Ok(value) => Ok(value),
            Err(BpError::Config(ConfigError::Missing { .. })) => {
                for alias in aliases {
                    match vars.required(alias) {
                        Ok(value) => return Ok(value),
                        Err(BpError::Config(ConfigError::Missing { .. })) => {}
                        Err(error) => return Err(error),
                    }
                }
                Err(BpError::config_missing(name))
            }
            Err(error) => Err(error),
        }
    }

    fn required_positive_duration(
        vars: &EnvVars,
        name: &str,
        aliases: &[&str],
    ) -> BpResult<Duration> {
        let value = Self::required_any(vars, name, aliases)?;
        let seconds = value
            .parse::<u64>()
            .map_err(|error| BpError::config_invalid_source(name, error))?;
        if seconds == 0 {
            return Err(BpError::config_invalid(name, "must be greater than zero"));
        }
        Ok(Duration::from_secs(seconds))
    }
}
