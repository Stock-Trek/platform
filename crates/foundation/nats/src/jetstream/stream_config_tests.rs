use crate::{NatsStreamConfig, StreamDefaults};
use platform_config::EnvVars;
use std::time::Duration;

const PREFIX: &str = "TEST_STREAM";
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const DEFAULT_MAX_BYTES: i64 = 1024 * 1024 * 1024;
const DEFAULT_MAX_MESSAGES: i64 = 10_000_000;

struct Fixture;

impl Fixture {
    fn defaults() -> StreamDefaults {
        StreamDefaults::new(DEFAULT_MAX_AGE, DEFAULT_MAX_BYTES, DEFAULT_MAX_MESSAGES)
    }

    fn config(entries: &[(&str, &str)]) -> NatsStreamConfig {
        let vars = EnvVars::from_map(
            entries
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        );
        NatsStreamConfig::from_vars(&vars, PREFIX, Self::defaults()).expect("stream config")
    }
}

#[test]
fn stream_config_uses_supplied_defaults() {
    let config = Fixture::config(&[]);

    assert_eq!(config.replicas(), 3);
    assert_eq!(config.max_age(), DEFAULT_MAX_AGE);
    assert_eq!(config.max_bytes(), DEFAULT_MAX_BYTES);
    assert_eq!(config.max_messages(), DEFAULT_MAX_MESSAGES);
    assert_eq!(config.duplicate_window(), Duration::from_secs(120));
}

#[test]
fn stream_config_reads_overrides() {
    let config = Fixture::config(&[
        ("TEST_STREAM_REPLICAS", "5"),
        ("TEST_STREAM_MAX_AGE_SECS", "3600"),
        ("TEST_STREAM_MAX_BYTES", "2048"),
        ("TEST_STREAM_MAX_MESSAGES", "4096"),
        ("TEST_STREAM_DUPLICATE_WINDOW_SECS", "60"),
    ]);

    assert_eq!(config.replicas(), 5);
    assert_eq!(config.max_age(), Duration::from_secs(3600));
    assert_eq!(config.max_bytes(), 2048);
    assert_eq!(config.max_messages(), 4096);
    assert_eq!(config.duplicate_window(), Duration::from_secs(60));
}

#[test]
fn stream_config_rejects_invalid_values() {
    for (name, value) in [
        ("TEST_STREAM_REPLICAS", "0"),
        ("TEST_STREAM_REPLICAS", "6"),
        ("TEST_STREAM_MAX_AGE_SECS", "0"),
        ("TEST_STREAM_MAX_BYTES", "0"),
        ("TEST_STREAM_MAX_MESSAGES", "0"),
        ("TEST_STREAM_DUPLICATE_WINDOW_SECS", "0"),
        ("TEST_STREAM_DUPLICATE_WINDOW_SECS", "2592001"),
    ] {
        let vars = EnvVars::from_map([(name.to_string(), value.to_string())]);
        assert!(
            NatsStreamConfig::from_vars(&vars, PREFIX, Fixture::defaults()).is_err(),
            "{name}={value}"
        );
    }
}
