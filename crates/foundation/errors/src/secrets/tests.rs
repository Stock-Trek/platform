use super::*;
use crate::Retryability;

#[test]
fn empty_secret_path_is_permanent() {
    let error = SecretsError::Empty {
        path: "/platform/missing".to_string(),
    };

    assert_eq!(error.retryability(), Retryability::Permanent);
}

#[test]
fn load_retryability_follows_the_ssm_error() {
    let error = |retryable| SecretsError::Load {
        operation: "SSM GetParametersByPath",
        path: "/platform/git-webhook/production".to_string(),
        retryable,
        source: "failure".into(),
    };

    assert_eq!(error(true).retryability(), Retryability::Retryable);
    assert_eq!(error(false).retryability(), Retryability::Permanent);
}
