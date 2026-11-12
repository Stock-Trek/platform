use super::*;

#[test]
fn predicates_match_the_retryability_variant() {
    assert!(Retryability::Retryable.is_retryable());
    assert!(!Retryability::Retryable.is_permanent());
    assert!(Retryability::Permanent.is_permanent());
    assert!(!Retryability::Permanent.is_retryable());
}

#[test]
fn permanent_s3_error_codes_are_not_retried() {
    for code in [
        "AccessDenied",
        "InvalidArgument",
        "NoSuchBucket",
        "NoSuchKey",
        "NotImplemented",
        "ObjectLockConfigurationNotFoundError",
    ] {
        assert_eq!(
            Retryability::from_s3_error_code(code),
            Retryability::Permanent,
            "{code}"
        );
    }
}

#[test]
fn permanent_ssm_error_codes_are_not_retried() {
    for code in [
        "AccessDenied",
        "AccessDeniedException",
        "InvalidFilterKey",
        "InvalidKeyId",
        "InvalidNextToken",
        "ParameterNotFound",
        "ValidationException",
    ] {
        assert_eq!(
            Retryability::from_ssm_error_code(code),
            Retryability::Permanent,
            "{code}"
        );
    }
}

#[test]
fn transient_ssm_error_codes_are_retried() {
    for code in [
        "InternalServerError",
        "RequestTimeout",
        "ServiceUnavailable",
        "ThrottlingException",
    ] {
        assert_eq!(
            Retryability::from_ssm_error_code(code),
            Retryability::Retryable,
            "{code}"
        );
    }
}

#[test]
fn transient_s3_error_codes_are_retried() {
    for code in [
        "InternalError",
        "RequestTimeout",
        "ServiceUnavailable",
        "SlowDown",
    ] {
        assert_eq!(
            Retryability::from_s3_error_code(code),
            Retryability::Retryable,
            "{code}"
        );
    }
}
