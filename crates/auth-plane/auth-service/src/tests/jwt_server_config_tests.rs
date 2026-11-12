use crate::JwtServerConfig;
use platform_config::EnvVars;
use platform_errors::BpResult;
use std::{collections::HashMap, time::Duration};

const BIND_ADDRESS: &str = "AUTH_SERVICE_BIND_ADDR";
const VAULT_ADDRESS: &str = "AUTH_SERVICE_VAULT_ADDRESS";
const VAULT_MOUNT: &str = "AUTH_SERVICE_VAULT_MOUNT";
const VAULT_TOKEN: &str = "AUTH_SERVICE_VAULT_TOKEN";
const LOG_FILTER: &str = "RUST_LOG";
const JWT_KEY_ID: &str = "AUTH_SERVICE_JWT_KEY_ID";
const JWT_PRIVATE_KEY_PEM: &str = "AUTH_SERVICE_JWT_PRIVATE_KEY_PEM";
const USAGE_QUEUE_URL: &str = "AUTH_SERVICE_USAGE_QUEUE_URL";
const USAGE_QUEUE_PREFIX: &str = "AUTH_SERVICE_USAGE_QUEUE_PREFIX";
const USAGE_QUEUE_TIMEOUT_SECS: &str = "AUTH_SERVICE_USAGE_QUEUE_TIMEOUT_SECS";
const USAGE_QUEUE_CAPACITY: &str = "AUTH_SERVICE_USAGE_QUEUE_CAPACITY";
const USAGE_QUEUE_BATCH_SIZE: &str = "AUTH_SERVICE_USAGE_QUEUE_BATCH_SIZE";
const VALKEY_URL: &str = "AUTH_SERVICE_VALKEY_URL";
const VALKEY_KEY_PREFIX: &str = "AUTH_SERVICE_VALKEY_KEY_PREFIX";
const VALKEY_TIMEOUT_SECS: &str = "AUTH_SERVICE_VALKEY_TIMEOUT_SECS";
const TOKEN_CACHE_MAX_ENTRIES: &str = "AUTH_SERVICE_TOKEN_CACHE_MAX_ENTRIES";
const TOKEN_CACHE_NEGATIVE_ENTRIES: &str = "AUTH_SERVICE_TOKEN_CACHE_NEGATIVE_ENTRIES";
const TOKEN_CACHE_FRESHNESS_SECS: &str = "AUTH_SERVICE_TOKEN_CACHE_FRESHNESS_SECS";
const TOKEN_CACHE_MAX_STALE_SECS: &str = "AUTH_SERVICE_TOKEN_CACHE_MAX_STALE_SECS";
const TOKEN_CACHE_FAILURE_THRESHOLD: &str = "AUTH_SERVICE_TOKEN_CACHE_FAILURE_THRESHOLD";
const TOKEN_CACHE_OPEN_SECS: &str = "AUTH_SERVICE_TOKEN_CACHE_OPEN_SECS";
const SIGV4_CREDENTIALS: &str = "SIGV4_CREDENTIALS";
const SIGV4_REGION: &str = "SIGV4_REGION";
const SIGV4_SERVICE: &str = "SIGV4_SERVICE";
const SIGV4_CREDENTIALS_JSON: &str =
    r#"[{"accessKeyId":"AKID","secretAccessKey":"secret","owner":"runner-owner"}]"#;

struct Fixture;

impl Fixture {
    fn key_pem() -> String {
        ed25519_compact::KeyPair::generate().sk.to_pem()
    }

    fn vars() -> HashMap<String, String> {
        HashMap::from([
            (LOG_FILTER.to_string(), "info".to_string()),
            (BIND_ADDRESS.to_string(), "127.0.0.1:8090".to_string()),
            (
                VAULT_ADDRESS.to_string(),
                "https://vault.example.com".to_string(),
            ),
            (VAULT_MOUNT.to_string(), "platform-tokens".to_string()),
            (VAULT_TOKEN.to_string(), "vault-token".to_string()),
            (JWT_KEY_ID.to_string(), "auth-service".to_string()),
            (JWT_PRIVATE_KEY_PEM.to_string(), Self::key_pem()),
            (
                USAGE_QUEUE_URL.to_string(),
                "redis://valkey.example.com:6379".to_string(),
            ),
            (USAGE_QUEUE_PREFIX.to_string(), "auth-usage".to_string()),
            (USAGE_QUEUE_TIMEOUT_SECS.to_string(), "5".to_string()),
            (USAGE_QUEUE_CAPACITY.to_string(), "1000".to_string()),
            (USAGE_QUEUE_BATCH_SIZE.to_string(), "100".to_string()),
            (
                VALKEY_URL.to_string(),
                "redis://valkey.example.com:6379".to_string(),
            ),
            (VALKEY_KEY_PREFIX.to_string(), "auth-replay".to_string()),
            (VALKEY_TIMEOUT_SECS.to_string(), "2".to_string()),
            (
                SIGV4_CREDENTIALS.to_string(),
                SIGV4_CREDENTIALS_JSON.to_string(),
            ),
            (SIGV4_REGION.to_string(), "eu-west-1".to_string()),
            (SIGV4_SERVICE.to_string(), "execute-api".to_string()),
        ])
    }

    fn config(extra: &[(&str, &str)]) -> BpResult<JwtServerConfig> {
        let mut vars = Self::vars();
        for (name, value) in extra {
            vars.insert((*name).to_string(), (*value).to_string());
        }
        JwtServerConfig::from_vars(&EnvVars::from_map(vars))
    }
}

#[test]
fn imports_a_pkcs8_private_key_from_the_environment() {
    let config = Fixture::config(&[(JWT_KEY_ID, "rotation-2026")]).unwrap();
    let signer = config.jwt_signing_key().unwrap();

    assert_eq!(config.jwt_key_id(), "rotation-2026");
    assert_eq!(signer.key_id(), "rotation-2026");
    assert_eq!(config.usage_queue().capacity(), 1000);
    assert_eq!(config.usage_queue().batch_size(), 100);
    assert_eq!(config.valkey().url(), "redis://valkey.example.com:6379");
    assert_eq!(config.valkey().key_prefix(), "auth-replay");
    assert_eq!(config.valkey().timeout(), Duration::from_secs(2));
}

#[test]
fn accepts_literal_newline_sequences_in_the_private_key() {
    let pem = Fixture::key_pem().replace('\n', "\\n");
    let config = Fixture::config(&[(JWT_PRIVATE_KEY_PEM, &pem)]).unwrap();

    assert!(config.jwt_signing_key().is_ok());
}

#[test]
fn rejects_a_malformed_private_key() {
    let config = Fixture::config(&[(JWT_PRIVATE_KEY_PEM, "not a pem key")]).unwrap();

    assert!(config.jwt_signing_key().is_err());
}

#[test]
fn configures_the_token_cache_from_the_environment() {
    let config = Fixture::config(&[
        (TOKEN_CACHE_MAX_ENTRIES, "42"),
        (TOKEN_CACHE_NEGATIVE_ENTRIES, "7"),
        (TOKEN_CACHE_FRESHNESS_SECS, "9"),
        (TOKEN_CACHE_MAX_STALE_SECS, "19"),
        (TOKEN_CACHE_FAILURE_THRESHOLD, "3"),
        (TOKEN_CACHE_OPEN_SECS, "27"),
    ])
    .unwrap();
    let cache = config.token_cache();

    assert_eq!(cache.max_entries(), 42);
    assert_eq!(cache.negative_entries(), 7);
    assert_eq!(cache.freshness(), Duration::from_secs(9));
    assert_eq!(cache.max_stale_age(), Duration::from_secs(19));
    assert_eq!(cache.failure_threshold(), 3);
    assert_eq!(cache.open_duration(), Duration::from_secs(27));
}

#[test]
fn token_cache_defaults_to_bounded_fresh_and_stale_windows() {
    let config = Fixture::config(&[]).unwrap();
    let cache = config.token_cache();

    assert_eq!(cache.max_entries(), 10_000);
    assert_eq!(cache.negative_entries(), 1_000);
    assert_eq!(cache.freshness(), Duration::from_secs(30));
    assert_eq!(cache.max_stale_age(), Duration::from_secs(60));
    assert_eq!(cache.failure_threshold(), 5);
    assert_eq!(cache.open_duration(), Duration::from_secs(10));
}

#[test]
fn rejects_a_stale_window_shorter_than_the_freshness_window() {
    assert!(
        Fixture::config(&[
            (TOKEN_CACHE_FRESHNESS_SECS, "30"),
            (TOKEN_CACHE_MAX_STALE_SECS, "10"),
        ])
        .is_err()
    );
}
