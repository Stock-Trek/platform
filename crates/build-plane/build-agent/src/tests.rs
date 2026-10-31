use super::*;
use platform_errors::{ConfigError, TimeoutError};
use std::time::Duration;

#[test]
fn permanent_build_errors_are_returned_as_structured_outcomes() {
    let error = BpError::Config(ConfigError::Build("compile failed".to_string()));

    let outcome = BuildAgent::handle_error(error, Instant::now() - Duration::from_secs(1)).unwrap();

    assert!(outcome.is_failure());
    let error = outcome.error().unwrap();
    assert_eq!(error.error_type, "build");
    assert!(error.message.contains("compile failed"));
    assert!(!error.retryable);
}

#[test]
fn retryable_build_errors_are_returned_so_lambda_retries_the_invocation() {
    let error = BpError::Timeout(TimeoutError::BuildTimeout(Duration::from_secs(300)));

    let error =
        BuildAgent::handle_error(error, Instant::now() - Duration::from_secs(1)).unwrap_err();

    assert_eq!(error.error_type(), "build_timeout");
    assert!(error.retryability().is_retryable());
}
