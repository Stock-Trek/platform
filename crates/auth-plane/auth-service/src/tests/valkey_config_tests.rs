use crate::{ValkeyReplayStoreConfig, valkey_config::DEFAULT_VALKEY_REPLAY_PREFIX};
use std::time::Duration;

struct ValkeyConfigFixture;

impl ValkeyConfigFixture {
    fn config(url: &str, prefix: &str, timeout: Duration) -> ValkeyReplayStoreConfig {
        ValkeyReplayStoreConfig::new(url, prefix, timeout).expect("valid configuration")
    }
}

#[test]
fn builds_a_configuration_with_an_explicit_prefix() {
    let config = ValkeyConfigFixture::config(
        "redis://valkey.example.com:6379",
        "replay:test",
        Duration::from_secs(3),
    );

    assert_eq!(config.url(), "redis://valkey.example.com:6379");
    assert_eq!(config.key_prefix(), "replay:test");
    assert_eq!(config.timeout(), Duration::from_secs(3));
}

#[test]
fn defaults_the_prefix_when_built_from_a_url() {
    let config = ValkeyReplayStoreConfig::from_url(
        "rediss://valkey.example.com:6380",
        Duration::from_secs(1),
    )
    .expect("valid configuration");

    assert_eq!(config.key_prefix(), DEFAULT_VALKEY_REPLAY_PREFIX);
}

#[test]
fn accepts_redis_and_valkey_transports_and_credentials() {
    for url in [
        "redis://127.0.0.1:6379",
        "rediss://user:password@valkey.example.com:6380",
        "valkey://valkey.example.com:6379",
        "valkeys://valkey.example.com:6380",
    ] {
        assert!(ValkeyReplayStoreConfig::new(url, "auth-replay", Duration::from_secs(1)).is_ok());
    }
}

#[test]
fn rejects_invalid_urls() {
    for url in [
        "",
        "not a url",
        "http://valkey.example.com",
        "redis://",
        "redis://valkey.example.com/#fragment",
        "redis://valkey.example.com/?protocol=resp3",
    ] {
        assert!(
            ValkeyReplayStoreConfig::new(url, "auth-replay", Duration::from_secs(1)).is_err(),
            "{url}"
        );
    }
}

#[test]
fn rejects_invalid_prefixes() {
    for prefix in [
        "",
        " ",
        "auth replay",
        "../auth-replay",
        "auth-replay:",
        ":auth-replay",
        "auth/replay",
    ] {
        assert!(
            ValkeyReplayStoreConfig::new(
                "redis://valkey.example.com:6379",
                prefix,
                Duration::from_secs(1),
            )
            .is_err(),
            "{prefix}"
        );
    }
    let long = "a".repeat(129);
    assert!(
        ValkeyReplayStoreConfig::new(
            "redis://valkey.example.com:6379",
            long,
            Duration::from_secs(1),
        )
        .is_err()
    );
}

#[test]
fn rejects_a_zero_timeout() {
    assert!(
        ValkeyReplayStoreConfig::new(
            "redis://valkey.example.com:6379",
            "auth-replay",
            Duration::ZERO,
        )
        .is_err()
    );
}

#[test]
fn debug_output_redacts_url_credentials() {
    let config = ValkeyConfigFixture::config(
        "rediss://user:super-secret@valkey.example.com:6380",
        "auth-replay",
        Duration::from_secs(1),
    );

    let debug = format!("{config:?}");
    assert!(!debug.contains("super-secret"));
    assert!(!debug.contains("user"));
    assert!(debug.contains("redacted"));
}
