use platform_errors::BpResult;
use std::{collections::HashMap, time::Duration};

use super::invoker::InvokerConfig;
use crate::cwasm::{
    DEFAULT_CWASM_DOWNLOAD_TIMEOUT, DEFAULT_EXECUTION_TIMEOUT, DEFAULT_MAX_CWASM_BYTES,
    DEFAULT_MAX_GUEST_MEMORY_BYTES, DEFAULT_MAX_TABLE_ELEMENTS,
};

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
