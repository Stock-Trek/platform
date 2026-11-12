use platform_config::EnvVars;
use platform_errors::{BuildError, BuildResult, ConfigError};
use std::time::Duration;

use super::invoker::InvokerConfig;

const EXECUTION_TIMEOUT_SECS: &str = "CWASM_INVOKER_EXECUTION_TIMEOUT_SECS";
const MAX_GUEST_MEMORY_BYTES: &str = "CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES";
const MAX_TABLE_ELEMENTS: &str = "CWASM_INVOKER_MAX_TABLE_ELEMENTS";
const CWASM_DOWNLOAD_TIMEOUT_SECS: &str = "CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS";
const MAX_CWASM_BYTES: &str = "CWASM_INVOKER_MAX_CWASM_BYTES";
const SIGV4_CREDENTIALS: &str = "SIGV4_CREDENTIALS";
const SIGV4_REGION: &str = "SIGV4_REGION";
const SIGV4_SERVICE: &str = "SIGV4_SERVICE";
const SIGV4_CREDENTIALS_JSON: &str =
    r#"[{"accessKeyId":"AKID","secretAccessKey":"secret","owner":"runner-owner"}]"#;

const ALL_VARS: [(&str, &str); 8] = [
    (EXECUTION_TIMEOUT_SECS, "45"),
    (MAX_GUEST_MEMORY_BYTES, "1048576"),
    (MAX_TABLE_ELEMENTS, "2048"),
    (CWASM_DOWNLOAD_TIMEOUT_SECS, "15"),
    (MAX_CWASM_BYTES, "4096"),
    (SIGV4_CREDENTIALS, SIGV4_CREDENTIALS_JSON),
    (SIGV4_REGION, "eu-west-1"),
    (SIGV4_SERVICE, "execute-api"),
];

struct Fixture;

impl Fixture {
    fn config(vars: &[(&str, &str)]) -> BuildResult<InvokerConfig> {
        InvokerConfig::from_vars(&EnvVars::from_map(
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

    assert_eq!(config.execution.timeout, Duration::from_secs(45));
    assert_eq!(config.execution.max_memory_bytes, 1024 * 1024);
    assert_eq!(config.execution.max_table_elements, 2048);
    assert_eq!(config.cwasm.download_timeout, Duration::from_secs(15));
    assert_eq!(config.cwasm.max_bytes.get(), 4096);
    assert_eq!(config.sigv4.region(), "eu-west-1");
    assert_eq!(config.sigv4.service(), "execute-api");
}

#[test]
fn requires_every_variable() {
    for (name, _) in ALL_VARS {
        assert!(matches!(
            Fixture::config(&Fixture::without(name)).unwrap_err(),
            BuildError::Config(ConfigError::Missing { name: missing }) if missing == name
        ));
    }
}

#[test]
fn rejects_empty_values() {
    for (name, _) in ALL_VARS {
        assert!(matches!(
            Fixture::config(&Fixture::with(name, "")).unwrap_err(),
            BuildError::Config(ConfigError::Empty { name: empty }) if empty == name
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
        (SIGV4_CREDENTIALS, "not-json"),
        (SIGV4_CREDENTIALS, "[]"),
    ] {
        assert!(
            Fixture::config(&Fixture::with(name, value)).is_err(),
            "{name}={value}"
        );
    }
}
