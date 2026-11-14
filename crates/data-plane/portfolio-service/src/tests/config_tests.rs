use crate::PortfolioConfig;
use platform_config::EnvVars;
use platform_errors::{ConfigError, PlatformError, PlatformResult};
use std::{net::SocketAddr, time::Duration};

const LOG_FILTER: &str = "RUST_LOG";
const BIND_ADDRESS: &str = "PORTFOLIO_ORDER_REQUEST_BIND_ADDR";
const DELIVERY_URL: &str = "PORTFOLIO_ORDER_REQUEST_URL";
const VALKEY_URL: &str = "PORTFOLIO_VALKEY_URL";
const VALKEY_KEY_PREFIX: &str = "PORTFOLIO_VALKEY_KEY_PREFIX";
const VALKEY_TIMEOUT_SECS: &str = "PORTFOLIO_VALKEY_TIMEOUT_SECS";
const VAULT_ADDRESS: &str = "PORTFOLIO_VAULT_ADDRESS";
const VAULT_TOKEN: &str = "PORTFOLIO_VAULT_TOKEN";
const VAULT_MOUNT: &str = "PORTFOLIO_VAULT_MOUNT";
const JWKS_URL: &str = "PORTFOLIO_JWKS_URL";
const USAGE_QUEUE_URL: &str = "PORTFOLIO_USAGE_QUEUE_URL";
const USAGE_QUEUE_PREFIX: &str = "PORTFOLIO_USAGE_QUEUE_PREFIX";
const USAGE_QUEUE_TIMEOUT_SECS: &str = "PORTFOLIO_USAGE_QUEUE_TIMEOUT_SECS";
const USAGE_QUEUE_CAPACITY: &str = "PORTFOLIO_USAGE_QUEUE_CAPACITY";
const USAGE_QUEUE_BATCH_SIZE: &str = "PORTFOLIO_USAGE_QUEUE_BATCH_SIZE";
const TOKEN_CACHE_MAX_ENTRIES: &str = "PORTFOLIO_TOKEN_CACHE_MAX_ENTRIES";
const TOKEN_CACHE_NEGATIVE_ENTRIES: &str = "PORTFOLIO_TOKEN_CACHE_NEGATIVE_ENTRIES";
const TOKEN_CACHE_FRESHNESS_SECS: &str = "PORTFOLIO_TOKEN_CACHE_FRESHNESS_SECS";
const TOKEN_CACHE_MAX_STALE_SECS: &str = "PORTFOLIO_TOKEN_CACHE_MAX_STALE_SECS";
const TOKEN_CACHE_FAILURE_THRESHOLD: &str = "PORTFOLIO_TOKEN_CACHE_FAILURE_THRESHOLD";
const TOKEN_CACHE_OPEN_SECS: &str = "PORTFOLIO_TOKEN_CACHE_OPEN_SECS";
const OWNERSHIP_NATS_URL: &str = "PORTFOLIO_OWNERSHIP_NATS_URL";
const OWNERSHIP_CLIENT_NAME: &str = "PORTFOLIO_OWNERSHIP_CLIENT_NAME";
const OWNERSHIP_DURABLE: &str = "PORTFOLIO_OWNERSHIP_DURABLE";
const OWNERSHIP_CHECKPOINT_BUCKET: &str = "PORTFOLIO_OWNERSHIP_CHECKPOINT_BUCKET";
const OWNERSHIP_RETRY_BACKOFF_SECS: &str = "PORTFOLIO_OWNERSHIP_RETRY_BACKOFF_SECS";
const UPDATE_NATS_URL: &str = "PORTFOLIO_UPDATE_NATS_URL";
const UPDATE_CLIENT_NAME: &str = "PORTFOLIO_UPDATE_CLIENT_NAME";
const UPDATE_DURABLE: &str = "PORTFOLIO_UPDATE_DURABLE";
const UPDATE_CHECKPOINT_BUCKET: &str = "PORTFOLIO_UPDATE_CHECKPOINT_BUCKET";
const UPDATE_RETRY_BACKOFF_SECS: &str = "PORTFOLIO_UPDATE_RETRY_BACKOFF_SECS";

const ALL_VARS: [(&str, &str); 25] = [
    (LOG_FILTER, "info"),
    (BIND_ADDRESS, "127.0.0.1:8080"),
    (DELIVERY_URL, "https://orders.example.com/orders"),
    (JWKS_URL, "https://auth.example.com/.well-known/jwks.json"),
    (VALKEY_URL, "redis://valkey.example.com:6379"),
    (VALKEY_KEY_PREFIX, "auth-replay"),
    (VALKEY_TIMEOUT_SECS, "2"),
    (VAULT_ADDRESS, "https://vault.example.com"),
    (VAULT_TOKEN, "vault-token"),
    (VAULT_MOUNT, "platform-tokens"),
    (USAGE_QUEUE_URL, "redis://valkey.example.com:6379"),
    (USAGE_QUEUE_PREFIX, "auth-usage"),
    (USAGE_QUEUE_TIMEOUT_SECS, "5"),
    (USAGE_QUEUE_CAPACITY, "1000"),
    (USAGE_QUEUE_BATCH_SIZE, "100"),
    (OWNERSHIP_NATS_URL, "nats://nats.example.com:4222"),
    (OWNERSHIP_CLIENT_NAME, "portfolio-service"),
    (OWNERSHIP_DURABLE, "portfolio-ownership"),
    (OWNERSHIP_CHECKPOINT_BUCKET, "portfolio-checkpoints"),
    (OWNERSHIP_RETRY_BACKOFF_SECS, "3"),
    (UPDATE_NATS_URL, "nats://nats.example.com:4222"),
    (UPDATE_CLIENT_NAME, "portfolio-service-updates"),
    (UPDATE_DURABLE, "portfolio-updates"),
    (UPDATE_CHECKPOINT_BUCKET, "portfolio-checkpoints"),
    (UPDATE_RETRY_BACKOFF_SECS, "2"),
];

pub struct Fixture;

impl Fixture {
    pub fn config(vars: &[(&str, &str)]) -> PlatformResult<PortfolioConfig> {
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
        config.auth().valkey().url(),
        "redis://valkey.example.com:6379"
    );
    assert_eq!(config.auth().valkey().key_prefix(), "auth-replay");
    assert_eq!(config.auth().valkey().timeout(), Duration::from_secs(2));
    assert_eq!(config.auth().vault().address(), "https://vault.example.com");
    assert_eq!(config.auth().vault().mount(), "platform-tokens");
    assert_eq!(config.auth().vault().timeout(), Duration::from_secs(10));
    assert_eq!(config.auth().usage_queue().capacity(), 1000);
    assert_eq!(config.auth().usage_queue().batch_size(), 100);
    assert_eq!(
        config.auth().jwks_url(),
        "https://auth.example.com/.well-known/jwks.json"
    );
    assert_eq!(
        config.ownership().nats_url(),
        "nats://nats.example.com:4222"
    );
    assert_eq!(config.ownership().client_name(), "portfolio-service");
    assert_eq!(config.ownership().durable(), "portfolio-ownership");
    assert_eq!(
        config.ownership().checkpoint_bucket(),
        "portfolio-checkpoints"
    );
    assert_eq!(config.ownership().retry_backoff(), Duration::from_secs(3));
    assert_eq!(config.ownership().stream().replicas(), 3);
    assert_eq!(
        config.ownership().stream().max_age(),
        Duration::from_secs(30 * 24 * 60 * 60)
    );
    assert_eq!(config.ownership().stream().max_bytes(), 1024 * 1024 * 1024);
    assert_eq!(config.ownership().stream().max_messages(), 10_000_000);
    assert_eq!(config.update().nats_url(), "nats://nats.example.com:4222");
    assert_eq!(config.update().client_name(), "portfolio-service-updates");
    assert_eq!(config.update().durable(), "portfolio-updates");
    assert_eq!(config.update().checkpoint_bucket(), "portfolio-checkpoints");
    assert_eq!(config.update().retry_backoff(), Duration::from_secs(2));
    assert_eq!(config.update().stream().replicas(), 3);
    assert_eq!(
        config.update().stream().max_age(),
        Duration::from_secs(7 * 24 * 60 * 60)
    );
    assert_eq!(
        config.update().stream().max_bytes(),
        10 * 1024 * 1024 * 1024
    );
    assert_eq!(config.update().stream().max_messages(), 100_000_000);
    assert_eq!(
        config.freshness().policy().max_age(),
        chrono::Duration::seconds(60)
    );
}

#[test]
fn reads_stream_provisioning_overrides() {
    let mut vars = ALL_VARS.to_vec();
    vars.extend([
        ("PORTFOLIO_OWNERSHIP_STREAM_REPLICAS", "5"),
        ("PORTFOLIO_OWNERSHIP_STREAM_MAX_AGE_SECS", "60"),
        ("PORTFOLIO_OWNERSHIP_STREAM_MAX_BYTES", "2048"),
        ("PORTFOLIO_OWNERSHIP_STREAM_MAX_MESSAGES", "20"),
        ("PORTFOLIO_OWNERSHIP_STREAM_DUPLICATE_WINDOW_SECS", "30"),
    ]);
    let config = Fixture::config(&vars).unwrap();

    assert_eq!(config.ownership().stream().replicas(), 5);
    assert_eq!(
        config.ownership().stream().max_age(),
        Duration::from_secs(60)
    );
    assert_eq!(config.ownership().stream().max_bytes(), 2048);
    assert_eq!(config.ownership().stream().max_messages(), 20);
    assert_eq!(
        config.ownership().stream().duplicate_window(),
        Duration::from_secs(30)
    );
}

#[test]
fn rejects_invalid_stream_provisioning_overrides() {
    for (name, value) in [
        ("PORTFOLIO_UPDATE_STREAM_REPLICAS", "0"),
        ("PORTFOLIO_UPDATE_STREAM_REPLICAS", "6"),
        ("PORTFOLIO_UPDATE_STREAM_MAX_AGE_SECS", "0"),
        ("PORTFOLIO_UPDATE_STREAM_MAX_BYTES", "0"),
        ("PORTFOLIO_UPDATE_STREAM_MAX_MESSAGES", "0"),
        ("PORTFOLIO_UPDATE_STREAM_DUPLICATE_WINDOW_SECS", "0"),
        (
            "PORTFOLIO_OWNERSHIP_STREAM_DUPLICATE_WINDOW_SECS",
            "2592001",
        ),
    ] {
        let mut vars = ALL_VARS.to_vec();
        vars.push((name, value));
        assert!(Fixture::config(&vars).is_err(), "{name}={value}");
    }
}

#[test]
fn requires_every_variable() {
    for (name, _) in ALL_VARS {
        assert!(matches!(
            Fixture::config(&Fixture::without(name)).unwrap_err(),
            PlatformError::Config(ConfigError::Missing { name: missing }) if missing == name
        ));
    }
}

#[test]
fn rejects_empty_values() {
    for (name, _) in ALL_VARS {
        assert!(matches!(
            Fixture::config(&Fixture::with(name, "")).unwrap_err(),
            PlatformError::Config(ConfigError::Empty { name: empty }) if empty == name
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
        (VALKEY_URL, "not a url"),
        (VALKEY_URL, "file:///tmp/valkey"),
        (VALKEY_KEY_PREFIX, "../etc"),
        (VALKEY_KEY_PREFIX, "auth replay"),
        (VALKEY_TIMEOUT_SECS, "0"),
        (VAULT_ADDRESS, "not a url"),
        (VAULT_ADDRESS, "file:///tmp/vault"),
        (VAULT_MOUNT, "../sys"),
        (JWKS_URL, "not a url"),
        (JWKS_URL, "file:///tmp/jwks.json"),
        (JWKS_URL, "ftp://auth.example.com/jwks"),
        (USAGE_QUEUE_URL, "not a url"),
        (USAGE_QUEUE_URL, "file:///tmp/valkey"),
        (USAGE_QUEUE_PREFIX, "../auth-usage"),
        (USAGE_QUEUE_TIMEOUT_SECS, "0"),
        (USAGE_QUEUE_CAPACITY, "0"),
        (USAGE_QUEUE_BATCH_SIZE, "0"),
        (OWNERSHIP_NATS_URL, "not a url"),
        (OWNERSHIP_NATS_URL, "http://nats.example.com"),
        (OWNERSHIP_RETRY_BACKOFF_SECS, "0"),
        (UPDATE_NATS_URL, "not a url"),
        (UPDATE_NATS_URL, "http://nats.example.com"),
        (UPDATE_RETRY_BACKOFF_SECS, "0"),
    ] {
        assert!(
            Fixture::config(&Fixture::with(name, value)).is_err(),
            "{name}={value}"
        );
    }
}

#[test]
fn configures_the_token_cache_from_the_environment() {
    let mut vars = ALL_VARS.to_vec();
    vars.extend([
        (TOKEN_CACHE_MAX_ENTRIES, "42"),
        (TOKEN_CACHE_NEGATIVE_ENTRIES, "7"),
        (TOKEN_CACHE_FRESHNESS_SECS, "9"),
        (TOKEN_CACHE_MAX_STALE_SECS, "19"),
        (TOKEN_CACHE_FAILURE_THRESHOLD, "3"),
        (TOKEN_CACHE_OPEN_SECS, "27"),
    ]);
    let config = Fixture::config(&vars).unwrap();
    let cache = config.auth().token_cache();

    assert_eq!(cache.max_entries(), 42);
    assert_eq!(cache.negative_entries(), 7);
    assert_eq!(cache.freshness(), Duration::from_secs(9));
    assert_eq!(cache.max_stale_age(), Duration::from_secs(19));
    assert_eq!(cache.failure_threshold(), 3);
    assert_eq!(cache.open_duration(), Duration::from_secs(27));
}

#[test]
fn token_cache_defaults_to_bounded_fresh_and_stale_windows() {
    let config = Fixture::config(&ALL_VARS).unwrap();
    let cache = config.auth().token_cache();

    assert_eq!(cache.max_entries(), 10_000);
    assert_eq!(cache.negative_entries(), 1_000);
    assert_eq!(cache.freshness(), Duration::from_secs(30));
    assert_eq!(cache.max_stale_age(), Duration::from_secs(60));
    assert_eq!(cache.failure_threshold(), 5);
    assert_eq!(cache.open_duration(), Duration::from_secs(10));
}
