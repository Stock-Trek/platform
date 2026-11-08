use super::*;
use crate::Retryability;
use std::time::Duration;

#[test]
fn unknown_token_is_permanent() {
    let error = AuthError::UnknownToken {
        token_id: "token-a".to_string(),
    };

    assert_eq!(error.retryability(), Retryability::Permanent);
}

#[test]
fn duplicate_token_is_permanent() {
    let error = AuthError::TokenExists {
        token_id: "token-a".to_string(),
    };

    assert_eq!(error.retryability(), Retryability::Permanent);
}

#[test]
fn concurrent_modification_is_retryable() {
    let error = AuthError::Conflict {
        token_id: "token-a".to_string(),
    };

    assert_eq!(error.retryability(), Retryability::Retryable);
}

#[test]
fn invalid_field_is_permanent() {
    let error = AuthError::InvalidField {
        field: "owner",
        reason: "must not be empty".to_string(),
    };

    assert_eq!(error.retryability(), Retryability::Permanent);
}

#[test]
fn authentication_failures_are_permanent() {
    let errors = [
        AuthError::InvalidSecret {
            token_id: "token-a".to_string(),
        },
        AuthError::Revoked {
            token_id: "token-a".to_string(),
        },
        AuthError::Expired {
            token_id: "token-a".to_string(),
        },
        AuthError::RotationOverlapEnded {
            token_id: "token-a".to_string(),
        },
        AuthError::MissingScope {
            token_id: "token-a".to_string(),
            scope: "orders:write".to_string(),
        },
    ];

    for error in errors {
        assert_eq!(error.retryability(), Retryability::Permanent);
    }
}

#[test]
fn store_retryability_follows_the_flag() {
    let error = |retryable| AuthError::Store {
        operation: "write",
        target: "tokens/token-a".to_string(),
        retryable,
        source: "failure".into(),
    };

    assert_eq!(error(true).retryability(), Retryability::Retryable);
    assert_eq!(error(false).retryability(), Retryability::Permanent);
}

#[test]
fn unavailable_is_retryable() {
    let error = AuthError::Unavailable {
        retry_after: Duration::from_secs(1),
    };

    assert_eq!(error.retryability(), Retryability::Retryable);
}
