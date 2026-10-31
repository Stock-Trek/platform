use super::*;
use crate::{ConfigError, TimeoutError};
use std::time::Duration;

#[test]
fn multiple_is_retryable_when_any_failure_is_retryable() {
    let error = BpError::multiple([
        BpError::Config(ConfigError::Missing {
            name: "test".to_string(),
        }),
        BpError::Timeout(TimeoutError::TimedOut),
    ]);

    assert!(error.retryability().is_retryable());
}

#[test]
fn multiple_is_permanent_when_every_failure_is_permanent() {
    let error = BpError::multiple([
        BpError::Config(ConfigError::Missing {
            name: "first".to_string(),
        }),
        BpError::Config(ConfigError::Missing {
            name: "second".to_string(),
        }),
    ]);

    assert!(!error.retryability().is_retryable());
}

#[test]
fn multiple_flattens_nested_failures() {
    let error = BpError::multiple([
        BpError::multiple([BpError::Timeout(TimeoutError::TimedOut)]),
        BpError::Config(ConfigError::Missing {
            name: "test".to_string(),
        }),
    ]);

    let BpError::Data(DataError::Multiple { failures }) = &error else {
        panic!("expected aggregated error");
    };
    assert_eq!(failures.len(), 2);
    assert!(error.retryability().is_retryable());
}

#[test]
fn multiple_display_includes_each_failure() {
    let error = BpError::multiple([
        BpError::Config(ConfigError::Missing {
            name: "first".to_string(),
        }),
        BpError::Timeout(TimeoutError::BuildTimeout(Duration::from_secs(1))),
    ]);

    let message = error.to_string();
    assert!(message.contains("first"));
    assert!(message.contains("build timed out"));
}
