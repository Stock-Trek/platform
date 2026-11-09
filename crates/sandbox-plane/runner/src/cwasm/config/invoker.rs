use platform_auth_service::{ValkeyReplayStoreConfig, VaultTokenStoreConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_types::url_scheme::UrlScheme;
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
const VALKEY_KEY_PREFIX: &str = "CWASM_INVOKER_VALKEY_KEY_PREFIX";
const VALKEY_TIMEOUT_SECS: &str = "CWASM_INVOKER_VALKEY_TIMEOUT_SECS";
const VAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct AuditConfig {
    pub queue_url: String,
    pub timeout: Duration,
}

impl AuditConfig {
    fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            queue_url: vars.required_url(AUDIT_QUEUE_URL, &UrlScheme::HTTP)?,
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
            vars.required_url(VAULT_ADDRESS, &UrlScheme::HTTP)?,
            SecretString::from(vars.required(VAULT_TOKEN)?),
            vars.required(VAULT_MOUNT)?,
            VAULT_TIMEOUT,
        )?;
        let valkey = ValkeyReplayStoreConfig::new(
            vars.required(VALKEY_URL)?,
            vars.required(VALKEY_KEY_PREFIX)?,
            vars.required_positive_duration_secs(VALKEY_TIMEOUT_SECS)?,
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
}
