use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult, ConfigError};
use std::time::Duration;

use super::invoker::InvokerConfig;

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

const ALL_VARS: [(&str, &str); 10] = [
    (EXECUTION_TIMEOUT_SECS, "45"),
    (MAX_GUEST_MEMORY_BYTES, "1048576"),
    (MAX_TABLE_ELEMENTS, "2048"),
    (CWASM_DOWNLOAD_TIMEOUT_SECS, "15"),
    (MAX_CWASM_BYTES, "4096"),
    (VAULT_ADDRESS, "https://vault.example.com"),
    (VAULT_TOKEN, "vault-token"),
    (VAULT_MOUNT, "platform-tokens"),
    (AUDIT_QUEUE_URL, "https://sqs.example.com/123/audit"),
    (AUDIT_TIMEOUT_SECS, "5"),
];

fn config(vars: &[(&str, &str)]) -> BpResult<InvokerConfig> {
    InvokerConfig::from_vars(&EnvVars::from_map(
        vars.iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
    ))
}

#[test]
fn reads_every_value_from_the_environment() {
    let config = config(&ALL_VARS).unwrap();

    assert_eq!(config.execution.timeout, Duration::from_secs(45));
    assert_eq!(config.execution.max_memory_bytes, 1024 * 1024);
    assert_eq!(config.execution.max_table_elements, 2048);
    assert_eq!(config.cwasm.download_timeout, Duration::from_secs(15));
    assert_eq!(config.cwasm.max_bytes.get(), 4096);
    assert_eq!(config.vault.address(), "https://vault.example.com");
    assert_eq!(config.vault.mount(), "platform-tokens");
    assert_eq!(config.vault.timeout(), Duration::from_secs(10));
    assert_eq!(config.audit.queue_url, "https://sqs.example.com/123/audit");
    assert_eq!(config.audit.timeout, Duration::from_secs(5));
}

#[test]
fn rejects_invalid_service_configuration() {
    for (name, value) in [
        (VAULT_ADDRESS, "not a url"),
        (VAULT_ADDRESS, "file:///tmp/vault"),
        (VAULT_MOUNT, "../sys"),
        (VAULT_MOUNT, ""),
        (AUDIT_QUEUE_URL, "not a url"),
        (AUDIT_QUEUE_URL, "file:///tmp/audit"),
    ] {
        let vars: Vec<(&str, &str)> = ALL_VARS
            .iter()
            .copied()
            .map(|(variable, old)| {
                if variable == name {
                    (variable, value)
                } else {
                    (variable, old)
                }
            })
            .collect();
        assert!(config(&vars).is_err(), "{name}={value}");
    }
}

#[test]
fn requires_every_variable() {
    for (name, _) in ALL_VARS {
        let vars: Vec<(&str, &str)> = ALL_VARS
            .iter()
            .copied()
            .filter(|(variable, _)| *variable != name)
            .collect();
        assert!(matches!(
            config(&vars).unwrap_err(),
            BpError::Config(ConfigError::Missing { name: missing }) if missing == name
        ));
    }
}

#[test]
fn rejects_empty_values() {
    for (name, _) in ALL_VARS {
        let vars: Vec<(&str, &str)> = ALL_VARS
            .iter()
            .copied()
            .map(|(variable, value)| {
                if variable == name {
                    (variable, "")
                } else {
                    (variable, value)
                }
            })
            .collect();
        assert!(matches!(
            config(&vars).unwrap_err(),
            BpError::Config(ConfigError::Empty { name: empty }) if empty == name
        ));
    }
}

#[test]
fn rejects_zero_and_invalid_values() {
    for (name, value) in [
        (EXECUTION_TIMEOUT_SECS, "0"),
        (MAX_GUEST_MEMORY_BYTES, "0"),
        (MAX_TABLE_ELEMENTS, "0"),
        (CWASM_DOWNLOAD_TIMEOUT_SECS, "0"),
        (MAX_CWASM_BYTES, "0"),
        (MAX_CWASM_BYTES, "not-a-number"),
        (AUDIT_TIMEOUT_SECS, "0"),
    ] {
        let vars: Vec<(&str, &str)> = ALL_VARS
            .iter()
            .copied()
            .map(|(variable, old)| {
                if variable == name {
                    (variable, value)
                } else {
                    (variable, old)
                }
            })
            .collect();
        assert!(config(&vars).is_err(), "{name}={value}");
    }
}
