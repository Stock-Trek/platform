use super::run_invoker::RunInvokerConfig;
use platform_config::EnvVars;
use std::time::Duration;

fn full_vars() -> Vec<(&'static str, &'static str)> {
    vec![
        ("RUST_LOG", "info"),
        ("AWS_CONNECT_TIMEOUT_SECS", "5"),
        ("AWS_OPERATION_TIMEOUT_SECS", "30"),
        ("RUN_INVOKER_TABLE", "runs"),
        ("RUN_INVOKER_ARTIFACT_BUCKET", "artifacts"),
        ("RUN_INVOKER_LOGS_BUCKET", "run-logs"),
        (
            "RUN_INVOKER_ORDER_REQUEST_URL",
            "https://orders.example/requests",
        ),
        ("RUN_INVOKER_AUTH_SERVICE_URL", "https://auth.example/jwt"),
        ("RUN_INVOKER_RUNNER_FUNCTION_NAME", "platform-runner"),
        ("RUN_INVOKER_MAX_BATCH", "10"),
        ("S3_SIGNED_URL_EXPIRY_SECS", "900"),
    ]
}

fn vars(entries: &[(&str, &str)]) -> EnvVars {
    EnvVars::from_map(entries.iter().copied())
}

fn vars_without(name: &str) -> EnvVars {
    let entries: Vec<(&str, &str)> = full_vars()
        .into_iter()
        .filter(|(key, _)| *key != name)
        .collect();
    vars(&entries)
}

fn vars_with(name: &str, value: &str) -> EnvVars {
    let mut entries: Vec<(String, String)> = full_vars()
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    for entry in &mut entries {
        if entry.0 == name {
            entry.1 = value.to_string();
        }
    }
    let entries: Vec<(&str, &str)> = entries
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    vars(&entries)
}

#[test]
fn reads_a_complete_configuration() {
    let config = RunInvokerConfig::from_vars(&vars(&full_vars())).unwrap();
    assert_eq!(config.log_filter, "info");
    assert_eq!(config.table, "runs");
    assert_eq!(config.artifact_bucket, "artifacts");
    assert_eq!(config.logs_bucket, "run-logs");
    assert_eq!(config.order_request_url, "https://orders.example/requests");
    assert_eq!(config.auth_service_url, "https://auth.example/jwt");
    assert_eq!(config.runner_function_name, "platform-runner");
    assert_eq!(config.max_batch, 10);
    assert_eq!(config.signed_url_expiry, Duration::from_secs(900));
    assert_eq!(
        config.aws_timeouts.connect.duration(),
        Some(Duration::from_secs(5))
    );
    assert_eq!(
        config.aws_timeouts.operation.duration(),
        Some(Duration::from_secs(30))
    );
}

#[test]
fn requires_every_variable() {
    for (name, _) in full_vars() {
        let vars = vars_without(name);
        assert!(
            RunInvokerConfig::from_vars(&vars).is_err(),
            "expected {name} to be required"
        );
    }
}

#[test]
fn rejects_invalid_values() {
    for (name, value) in [
        ("RUST_LOG", ""),
        ("AWS_CONNECT_TIMEOUT_SECS", "-5"),
        ("AWS_OPERATION_TIMEOUT_SECS", "soon"),
        ("RUN_INVOKER_TABLE", ""),
        ("RUN_INVOKER_ARTIFACT_BUCKET", ""),
        ("RUN_INVOKER_LOGS_BUCKET", ""),
        ("RUN_INVOKER_ORDER_REQUEST_URL", ""),
        ("RUN_INVOKER_AUTH_SERVICE_URL", ""),
        ("RUN_INVOKER_RUNNER_FUNCTION_NAME", ""),
        ("RUN_INVOKER_MAX_BATCH", "0"),
        ("RUN_INVOKER_MAX_BATCH", "ten"),
        ("S3_SIGNED_URL_EXPIRY_SECS", ""),
    ] {
        let vars = vars_with(name, value);
        assert!(
            RunInvokerConfig::from_vars(&vars).is_err(),
            "expected {name}={value:?} to be rejected"
        );
    }
}
