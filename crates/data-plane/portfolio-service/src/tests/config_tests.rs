use crate::PortfolioConfig;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult, ConfigError};
use std::{net::SocketAddr, time::Duration};

const LOG_FILTER: &str = "RUST_LOG";
const BIND_ADDRESS: &str = "PORTFOLIO_ORDER_REQUEST_BIND_ADDR";
const DELIVERY_URL: &str = "PORTFOLIO_ORDER_REQUEST_URL";
const AUDIT_QUEUE_URL: &str = "PORTFOLIO_AUDIT_QUEUE_URL";
const AUDIT_TIMEOUT_SECS: &str = "PORTFOLIO_AUDIT_TIMEOUT_SECS";
const VALKEY_URL: &str = "PORTFOLIO_VALKEY_URL";
const VALKEY_KEY_PREFIX: &str = "PORTFOLIO_VALKEY_KEY_PREFIX";
const VALKEY_TIMEOUT_SECS: &str = "PORTFOLIO_VALKEY_TIMEOUT_SECS";
const VAULT_ADDRESS: &str = "PORTFOLIO_VAULT_ADDRESS";
const VAULT_TOKEN: &str = "PORTFOLIO_VAULT_TOKEN";
const VAULT_MOUNT: &str = "PORTFOLIO_VAULT_MOUNT";

const ALL_VARS: [(&str, &str); 11] = [
    (LOG_FILTER, "info"),
    (BIND_ADDRESS, "127.0.0.1:8080"),
    (DELIVERY_URL, "https://orders.example.com/orders"),
    (AUDIT_QUEUE_URL, "https://sqs.example.com/123/audit"),
    (AUDIT_TIMEOUT_SECS, "5"),
    (VALKEY_URL, "redis://valkey.example.com:6379"),
    (VALKEY_KEY_PREFIX, "auth-replay"),
    (VALKEY_TIMEOUT_SECS, "2"),
    (VAULT_ADDRESS, "https://vault.example.com"),
    (VAULT_TOKEN, "vault-token"),
    (VAULT_MOUNT, "platform-tokens"),
];

struct Fixture;

impl Fixture {
    fn config(vars: &[(&str, &str)]) -> BpResult<PortfolioConfig> {
        PortfolioConfig::from_vars(&EnvVars::from_map(
            vars.iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        ))
    }

    fn with<'a>(variable: &'a str, value: &'a str) -> Vec<(&'a str, &'a str)> {
        ALL_VARS
            .iter()
            .copied()
            .map(|(name, old)| {
                if name == variable {
                    (name, value)
                } else {
                    (name, old)
                }
            })
            .collect()
    }

    fn without(variable: &str) -> Vec<(&str, &str)> {
        ALL_VARS
            .iter()
            .copied()
            .filter(|(name, _)| *name != variable)
            .collect()
    }
}

#[test]
fn reads_every_value_from_the_environment() {
    let config = Fixture::config(&ALL_VARS).unwrap();

    assert_eq!(config.log_filter(), "info");
    assert_eq!(
        config.order_request().bind_address(),
        "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(
        config.order_request().delivery_url(),
        "https://orders.example.com/orders"
    );
    assert_eq!(
        config.auth().audit().queue_url(),
        "https://sqs.example.com/123/audit"
    );
    assert_eq!(config.auth().audit().timeout(), Duration::from_secs(5));
    assert_eq!(
        config.auth().valkey().url(),
        "redis://valkey.example.com:6379"
    );
    assert_eq!(config.auth().valkey().key_prefix(), "auth-replay");
    assert_eq!(config.auth().valkey().timeout(), Duration::from_secs(2));
    assert_eq!(config.auth().vault().address(), "https://vault.example.com");
    assert_eq!(config.auth().vault().mount(), "platform-tokens");
    assert_eq!(config.auth().vault().timeout(), Duration::from_secs(10));
}

#[test]
fn requires_every_variable() {
    for (name, _) in ALL_VARS {
        assert!(matches!(
            Fixture::config(&Fixture::without(name)).unwrap_err(),
            BpError::Config(ConfigError::Missing { name: missing }) if missing == name
        ));
    }
}

#[test]
fn rejects_empty_values() {
    for (name, _) in ALL_VARS {
        assert!(matches!(
            Fixture::config(&Fixture::with(name, "")).unwrap_err(),
            BpError::Config(ConfigError::Empty { name: empty }) if empty == name
        ));
    }
}

#[test]
fn rejects_invalid_service_configuration() {
    for (name, value) in [
        (LOG_FILTER, "not a filter ["),
        (BIND_ADDRESS, "not an address"),
        (DELIVERY_URL, "not a url"),
        (DELIVERY_URL, "file:///tmp/orders"),
        (AUDIT_QUEUE_URL, "not a url"),
        (AUDIT_QUEUE_URL, "file:///tmp/audit"),
        (AUDIT_TIMEOUT_SECS, "0"),
        (VALKEY_URL, "not a url"),
        (VALKEY_URL, "file:///tmp/valkey"),
        (VALKEY_KEY_PREFIX, "../etc"),
        (VALKEY_KEY_PREFIX, "auth replay"),
        (VALKEY_TIMEOUT_SECS, "0"),
        (VAULT_ADDRESS, "not a url"),
        (VAULT_ADDRESS, "file:///tmp/vault"),
        (VAULT_MOUNT, "../sys"),
    ] {
        assert!(
            Fixture::config(&Fixture::with(name, value)).is_err(),
            "{name}={value}"
        );
    }
}
