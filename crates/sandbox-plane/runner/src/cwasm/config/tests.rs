use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult, ConfigError};
use secrecy::ExposeSecret;
use std::time::Duration;

use super::invoker::InvokerConfig;

const EXECUTION_TIMEOUT_SECS: &str = "CWASM_INVOKER_EXECUTION_TIMEOUT_SECS";
const MAX_GUEST_MEMORY_BYTES: &str = "CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES";
const MAX_TABLE_ELEMENTS: &str = "CWASM_INVOKER_MAX_TABLE_ELEMENTS";
const CWASM_DOWNLOAD_TIMEOUT_SECS: &str = "CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS";
const MAX_CWASM_BYTES: &str = "CWASM_INVOKER_MAX_CWASM_BYTES";
const ORDER_REQUEST_TOKEN_ID: &str = "CWASM_INVOKER_ORDER_REQUEST_TOKEN_ID";
const ORDER_REQUEST_SIGNING_SECRET: &str = "CWASM_INVOKER_ORDER_REQUEST_SIGNING_SECRET";

const ALL_VARS: [(&str, &str); 7] = [
    (EXECUTION_TIMEOUT_SECS, "45"),
    (MAX_GUEST_MEMORY_BYTES, "1048576"),
    (MAX_TABLE_ELEMENTS, "2048"),
    (CWASM_DOWNLOAD_TIMEOUT_SECS, "15"),
    (MAX_CWASM_BYTES, "4096"),
    (ORDER_REQUEST_TOKEN_ID, "runner-token"),
    (ORDER_REQUEST_SIGNING_SECRET, "runner-secret"),
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
    assert_eq!(config.order_request_token_id, "runner-token");
    assert_eq!(
        config.order_request_signing_secret.expose_secret(),
        "runner-secret"
    );
}

#[test]
fn rejects_invalid_order_request_token_ids() {
    let vars: Vec<(&str, &str)> = ALL_VARS
        .iter()
        .copied()
        .map(|(name, value)| {
            if name == ORDER_REQUEST_TOKEN_ID {
                (name, "not a valid token id")
            } else {
                (name, value)
            }
        })
        .collect();

    assert!(config(&vars).is_err());
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
