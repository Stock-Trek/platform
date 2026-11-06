use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;

use crate::cwasm::{CwasmLimits, ExecutionLimits};

const EXECUTION_TIMEOUT_SECS: &str = "CWASM_INVOKER_EXECUTION_TIMEOUT_SECS";
const MAX_GUEST_MEMORY_BYTES: &str = "CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES";
const MAX_TABLE_ELEMENTS: &str = "CWASM_INVOKER_MAX_TABLE_ELEMENTS";
const CWASM_DOWNLOAD_TIMEOUT_SECS: &str = "CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS";
const MAX_CWASM_BYTES: &str = "CWASM_INVOKER_MAX_CWASM_BYTES";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvokerConfig {
    pub execution: ExecutionLimits,
    pub cwasm: CwasmLimits,
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

        Ok(Self { execution, cwasm })
    }
}
