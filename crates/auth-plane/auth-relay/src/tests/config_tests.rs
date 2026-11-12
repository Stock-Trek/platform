use crate::AuthRelayConfig;
use platform_config::env_names::{AWS_CONNECT_TIMEOUT_SECS, AWS_OPERATION_TIMEOUT_SECS, RUST_LOG};
use platform_config::env_vars::EnvVars;
use platform_errors::{BuildError, BuildResult};
use std::time::Duration;

const AUDIT_QUEUE_URL: &str = "AUTH_RELAY_AUDIT_QUEUE_URL";
const AUDIT_TIMEOUT_SECS: &str = "AUTH_RELAY_AUDIT_TIMEOUT_SECS";
const VAULT_ADDRESS: &str = "AUTH_RELAY_VAULT_ADDRESS";
const VAULT_MOUNT: &str = "AUTH_RELAY_VAULT_MOUNT";
const VAULT_TIMEOUT_SECS: &str = "AUTH_RELAY_VAULT_TIMEOUT_SECS";
const VAULT_TOKEN: &str = "AUTH_RELAY_VAULT_TOKEN";
const USAGE_QUEUE_URL: &str = "AUTH_RELAY_USAGE_QUEUE_URL";
const USAGE_QUEUE_PREFIX: &str = "AUTH_RELAY_USAGE_QUEUE_PREFIX";
const USAGE_QUEUE_TIMEOUT_SECS: &str = "AUTH_RELAY_USAGE_QUEUE_TIMEOUT_SECS";
const USAGE_QUEUE_CAPACITY: &str = "AUTH_RELAY_USAGE_QUEUE_CAPACITY";
const USAGE_QUEUE_BATCH_SIZE: &str = "AUTH_RELAY_USAGE_QUEUE_BATCH_SIZE";

const ALL_VARS: [(&str, &str); 14] = [
    (RUST_LOG, "info"),
    (AWS_CONNECT_TIMEOUT_SECS, "5"),
    (AWS_OPERATION_TIMEOUT_SECS, "15"),
    (VAULT_ADDRESS, "https://vault.example.com"),
    (VAULT_TOKEN, "vault-token"),
    (VAULT_MOUNT, "platform-tokens"),
    (VAULT_TIMEOUT_SECS, "10"),
    (AUDIT_QUEUE_URL, "https://sqs.example.com/123/audit"),
    (AUDIT_TIMEOUT_SECS, "5"),
    (USAGE_QUEUE_URL, "redis://valkey.example.com:6379"),
    (USAGE_QUEUE_PREFIX, "auth-usage"),
    (USAGE_QUEUE_TIMEOUT_SECS, "5"),
    (USAGE_QUEUE_CAPACITY, "1000"),
    (USAGE_QUEUE_BATCH_SIZE, "100"),
];

struct ConfigFixture;

impl ConfigFixture {
    fn config(vars: &[(&str, &str)]) -> BuildResult<AuthRelayConfig> {
        AuthRelayConfig::from_vars(&EnvVars::from_map(
            vars.iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        ))
    }

    fn with(name: &str, value: &str) -> BuildResult<AuthRelayConfig> {
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
        Self::config(&vars)
    }
}

#[test]
fn reads_every_value_from_the_environment() {
    let config = ConfigFixture::config(&ALL_VARS).expect("valid config");

    assert_eq!(config.log_filter, "info");
    assert_eq!(config.vault.address(), "https://vault.example.com");
    assert_eq!(config.vault.mount(), "platform-tokens");
    assert_eq!(config.vault.timeout(), Duration::from_secs(10));
    assert_eq!(config.audit_queue_url, "https://sqs.example.com/123/audit");
    assert_eq!(config.audit_timeout, Duration::from_secs(5));
    assert_eq!(config.usage_queue.url(), "redis://valkey.example.com:6379");
    assert_eq!(config.usage_queue.key_prefix(), "auth-usage");
    assert_eq!(config.usage_queue.capacity(), 1000);
    assert_eq!(config.usage_queue.batch_size(), 100);
    assert_eq!(
        config.aws_timeouts.connect.duration(),
        Some(Duration::from_secs(5))
    );
    assert_eq!(
        config.aws_timeouts.operation.duration(),
        Some(Duration::from_secs(15))
    );
}

#[test]
fn rejects_invalid_service_configuration() {
    for (name, value) in [
        (VAULT_ADDRESS, "not a url"),
        (VAULT_ADDRESS, "file:///tmp/vault"),
        (VAULT_MOUNT, "../sys"),
        (AUDIT_QUEUE_URL, "not a url"),
        (AUDIT_QUEUE_URL, "file:///tmp/audit"),
        (VAULT_TIMEOUT_SECS, "0"),
        (AUDIT_TIMEOUT_SECS, "0"),
        (USAGE_QUEUE_URL, "not a url"),
        (USAGE_QUEUE_URL, "https://valkey.example.com"),
        (USAGE_QUEUE_PREFIX, "../auth-usage"),
        (USAGE_QUEUE_TIMEOUT_SECS, "0"),
        (USAGE_QUEUE_CAPACITY, "0"),
        (USAGE_QUEUE_BATCH_SIZE, "0"),
        (AWS_CONNECT_TIMEOUT_SECS, "0"),
        (AWS_OPERATION_TIMEOUT_SECS, "0"),
    ] {
        assert!(ConfigFixture::with(name, value).is_err(), "{name}={value}");
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
        let error = ConfigFixture::config(&vars).expect_err("missing variable");

        assert!(
            matches!(
                error,
                BuildError::Config(platform_errors::ConfigError::Missing { name: missing })
                    if missing == name
            ),
            "{name}"
        );
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
        let error = ConfigFixture::config(&vars).expect_err("empty variable");

        assert!(
            matches!(
                error,
                BuildError::Config(platform_errors::ConfigError::Empty { name: empty })
                    if empty == name
            ),
            "{name}"
        );
    }
}
