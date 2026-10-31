use super::*;
use crate::Retryability;

fn failure(key: &str, code: &str) -> S3DeleteObjectFailure {
    S3DeleteObjectFailure {
        key: key.to_string(),
        code: code.to_string(),
        message: None,
    }
}

#[test]
fn delete_objects_with_only_permanent_failures_is_permanent() {
    let error = StorageError::S3DeleteObjects {
        target: "s3://bucket/2 objects".to_string(),
        failures: vec![
            failure("first", "AccessDenied"),
            failure("second", "NoSuchKey"),
        ],
    };

    assert_eq!(error.retryability(), Retryability::Permanent);
}

#[test]
fn delete_objects_with_any_retryable_failure_is_retryable() {
    let error = StorageError::S3DeleteObjects {
        target: "s3://bucket/2 objects".to_string(),
        failures: vec![
            failure("first", "AccessDenied"),
            failure("second", "SlowDown"),
        ],
    };

    assert_eq!(error.retryability(), Retryability::Retryable);
}

#[test]
fn delete_objects_with_only_retryable_failures_is_retryable() {
    let error = StorageError::S3DeleteObjects {
        target: "s3://bucket/1 objects".to_string(),
        failures: vec![failure("first", "InternalError")],
    };

    assert_eq!(error.retryability(), Retryability::Retryable);
}

#[test]
fn delete_objects_display_includes_each_failure() {
    let error = StorageError::S3DeleteObjects {
        target: "s3://bucket/1 objects".to_string(),
        failures: vec![S3DeleteObjectFailure {
            key: "first".to_string(),
            code: "AccessDenied".to_string(),
            message: Some("denied".to_string()),
        }],
    };

    let message = error.to_string();
    assert!(message.contains("first: AccessDenied (denied)"));
}
