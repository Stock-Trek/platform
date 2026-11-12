use crate::tests::support_tests::TestFixture;
use crate::{
    ApiToken,
    api_token::{MAX_OWNER_LENGTH, MAX_SECRET_LENGTH, MAX_TOKEN_ID_LENGTH},
};
use chrono::Utc;
use platform_errors::{AuthError, BuildError};
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;

#[test]
fn matches_only_the_exact_secret() {
    let token = TestFixture::token("token-a");

    assert!(token.matches_secret(token.secret().expose_secret()));
    assert!(!token.matches_secret("not-the-secret"));
}

#[test]
fn secrets_are_redacted_in_debug_output() {
    let token = TestFixture::token("token-a");

    assert!(!format!("{token:?}").contains(token.secret().expose_secret()));
}

#[test]
fn round_trips_through_a_vault_value() {
    let token = TestFixture::token("token-a");
    let value = token.to_value();

    let restored = ApiToken::from_value(token.id(), &value).unwrap();

    assert_eq!(restored.id(), token.id());
    assert_eq!(restored.owner(), token.owner());
    assert_eq!(restored.created_at(), token.created_at());
    assert_eq!(
        restored.secret().expose_secret(),
        token.secret().expose_secret()
    );
}

#[test]
fn rejects_invalid_token_ids() {
    let error = ApiToken::generate(
        "not/a/path",
        "demo-owner",
        Utc::now(),
        TestFixture::metadata(),
    );

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[test]
fn rejects_overlong_token_ids() {
    let too_long = "a".repeat(MAX_TOKEN_ID_LENGTH + 1);
    let error = ApiToken::generate(too_long, "demo-owner", Utc::now(), TestFixture::metadata());

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        }))
    ));
}

#[test]
fn rejects_overlong_owners_and_secrets() {
    let owner = "a".repeat(MAX_OWNER_LENGTH + 1);
    let owner_error = ApiToken::generate("token-a", owner, Utc::now(), TestFixture::metadata());
    assert!(matches!(
        owner_error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "owner",
            ..
        }))
    ));

    let secret = SecretString::from("a".repeat(MAX_SECRET_LENGTH + 1));
    let secret_error = ApiToken::from_parts(
        "token-a",
        "demo-owner",
        secret,
        Utc::now(),
        TestFixture::metadata(),
    );
    assert!(matches!(
        secret_error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "secret",
            ..
        }))
    ));
}

#[test]
fn rejects_control_characters_in_owners() {
    let error = ApiToken::generate(
        "token-a",
        "demo\nowner",
        Utc::now(),
        TestFixture::metadata(),
    );

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "owner",
            ..
        }))
    ));
}

#[test]
fn rejects_a_value_for_a_different_token_id() {
    let token = TestFixture::token("token-a");

    let error = ApiToken::from_value("token-b", &token.to_value()).unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::InvalidField { field: "id", .. })
    ));
}

#[test]
fn rejects_missing_vault_value_fields() {
    let error = ApiToken::from_value("token-a", &json!({"owner": "demo-owner"}));

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[test]
fn rejects_blank_secrets() {
    for secret in ["", " ", "\t"] {
        let error = ApiToken::from_parts(
            "token-a",
            "demo-owner",
            SecretString::from(secret),
            Utc::now(),
            TestFixture::metadata(),
        );

        assert!(matches!(
            error,
            Err(BuildError::Auth(AuthError::InvalidField {
                field: "secret",
                ..
            }))
        ));
    }
}

#[test]
fn rejects_control_characters_in_secrets() {
    let error = ApiToken::from_parts(
        "token-a",
        "demo-owner",
        SecretString::from("secret\nvalue"),
        Utc::now(),
        TestFixture::metadata(),
    );

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "secret",
            ..
        }))
    ));
}
