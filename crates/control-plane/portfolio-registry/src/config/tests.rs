use super::PortfolioRegistryConfig;
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use std::time::Duration;

const LOG_FILTER: &str = "RUST_LOG";
const NATS_URL: &str = "PORTFOLIO_REGISTRY_NATS_URL";
const NATS_CLIENT_NAME: &str = "PORTFOLIO_REGISTRY_NATS_CLIENT_NAME";
const OUTBOX_INTERVAL: &str = "PORTFOLIO_REGISTRY_OUTBOX_INTERVAL_SECS";
const TABLE: &str = "PORTFOLIO_REGISTRY_TABLE";
const ADMIN_BIND: &str = "PORTFOLIO_REGISTRY_ADMIN_BIND_ADDR";
const ADMIN_TOKEN: &str = "PORTFOLIO_REGISTRY_ADMIN_TOKEN";
const READ_BIND: &str = "PORTFOLIO_REGISTRY_READ_BIND_ADDR";
const JWKS_URL: &str = "PORTFOLIO_REGISTRY_JWKS_URL";
const STREAM_REPLICAS: &str = "PORTFOLIO_OWNERSHIP_STREAM_REPLICAS";
const STREAM_MAX_AGE: &str = "PORTFOLIO_OWNERSHIP_STREAM_MAX_AGE_SECS";
const STREAM_MAX_BYTES: &str = "PORTFOLIO_OWNERSHIP_STREAM_MAX_BYTES";
const STREAM_MAX_MESSAGES: &str = "PORTFOLIO_OWNERSHIP_STREAM_MAX_MESSAGES";
const STREAM_DUPLICATE_WINDOW: &str = "PORTFOLIO_OWNERSHIP_STREAM_DUPLICATE_WINDOW_SECS";

const ALL_VARS: [(&str, &str); 9] = [
    (LOG_FILTER, "info"),
    (NATS_URL, "nats://nats.example.com:4222"),
    (NATS_CLIENT_NAME, "portfolio-registry"),
    (OUTBOX_INTERVAL, "2"),
    (TABLE, "portfolio-registry"),
    (ADMIN_BIND, "127.0.0.1:8080"),
    (ADMIN_TOKEN, "0123456789abcdef0123456789abcdef"),
    (READ_BIND, "127.0.0.1:8081"),
    (JWKS_URL, "https://auth.example.com/.well-known/jwks.json"),
];

struct Fixture;

impl Fixture {
    fn config(vars: &[(&str, &str)]) -> BpResult<PortfolioRegistryConfig> {
        PortfolioRegistryConfig::from_vars(&EnvVars::from_map(
            vars.iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        ))
    }
}

#[test]
fn ownership_stream_defaults_are_bounded() {
    let config = Fixture::config(&ALL_VARS).unwrap();
    let stream = config.nats().stream();

    assert_eq!(stream.replicas(), 3);
    assert_eq!(stream.max_age(), Duration::from_secs(30 * 24 * 60 * 60));
    assert_eq!(stream.max_bytes(), 1024 * 1024 * 1024);
    assert_eq!(stream.max_messages(), 10_000_000);
    assert_eq!(stream.duplicate_window(), Duration::from_secs(120));
}

#[test]
fn ownership_stream_settings_are_configurable() {
    let mut vars = ALL_VARS.to_vec();
    vars.extend([
        (STREAM_REPLICAS, "5"),
        (STREAM_MAX_AGE, "3600"),
        (STREAM_MAX_BYTES, "2048"),
        (STREAM_MAX_MESSAGES, "4096"),
        (STREAM_DUPLICATE_WINDOW, "60"),
    ]);
    let config = Fixture::config(&vars).unwrap();
    let stream = config.nats().stream();

    assert_eq!(stream.replicas(), 5);
    assert_eq!(stream.max_age(), Duration::from_secs(3600));
    assert_eq!(stream.max_bytes(), 2048);
    assert_eq!(stream.max_messages(), 4096);
    assert_eq!(stream.duplicate_window(), Duration::from_secs(60));
}

#[test]
fn invalid_ownership_stream_settings_are_rejected() {
    for (name, value) in [
        (STREAM_REPLICAS, "0"),
        (STREAM_REPLICAS, "6"),
        (STREAM_MAX_AGE, "0"),
        (STREAM_MAX_BYTES, "0"),
        (STREAM_MAX_MESSAGES, "0"),
        (STREAM_DUPLICATE_WINDOW, "0"),
        (STREAM_DUPLICATE_WINDOW, "2592001"),
    ] {
        let mut vars = ALL_VARS.to_vec();
        vars.push((name, value));
        assert!(Fixture::config(&vars).is_err(), "{name}={value}");
    }
}
