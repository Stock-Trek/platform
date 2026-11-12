use crate::{BuildError, ConfigError, DataError, TimeoutError};
use std::time::Duration;

#[test]
fn multiple_is_retryable_when_any_failure_is_retryable() {
    let error = BuildError::multiple([
        BuildError::Config(ConfigError::Missing {
            name: "test".to_string(),
        }),
        BuildError::Timeout(TimeoutError::TimedOut),
    ]);

    assert!(error.retryability().is_retryable());
}

#[test]
fn multiple_is_permanent_when_every_failure_is_permanent() {
    let error = BuildError::multiple([
        BuildError::Config(ConfigError::Missing {
            name: "first".to_string(),
        }),
        BuildError::Config(ConfigError::Missing {
            name: "second".to_string(),
        }),
    ]);

    assert!(!error.retryability().is_retryable());
}

#[test]
fn multiple_flattens_nested_failures() {
    let error = BuildError::multiple([
        BuildError::multiple([BuildError::Timeout(TimeoutError::TimedOut)]),
        BuildError::Config(ConfigError::Missing {
            name: "test".to_string(),
        }),
    ]);

    let BuildError::Data(DataError::Multiple { failures }) = &error else {
        panic!("expected aggregated error");
    };
    assert_eq!(failures.len(), 2);
    assert!(error.retryability().is_retryable());
}

#[test]
fn multiple_display_includes_each_failure() {
    let error = BuildError::multiple([
        BuildError::Config(ConfigError::Missing {
            name: "first".to_string(),
        }),
        BuildError::Timeout(TimeoutError::BuildTimeout(Duration::from_secs(1))),
    ]);

    let message = error.to_string();
    assert!(message.contains("first"));
    assert!(message.contains("build timed out"));
}
