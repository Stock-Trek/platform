use super::support_tests::TestFixture;
use crate::{ApiToken, api_token::MAX_TOKEN_ID_LENGTH};
use chrono::Utc;
use platform_errors::{AuthError, BpError};
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;

#[test]
fn each_token_gets_a_distinct_alphanumeric_secret() {
    let first = TestFixture::token("token-a");
    let second = TestFixture::token("token-b");

    let first_secret = first.secret().expose_secret();
    let second_secret = second.secret().expose_secret();
    assert_ne!(first_secret, second_secret);
    assert_eq!(first_secret.len(), 32);
    assert!(first_secret.chars().all(|c| c.is_ascii_alphanumeric()));
}

#[test]
fn secrets_are_matched_in_constant_time_comparison() {
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
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[test]
fn rejects_overlong_token_ids() {
    let too_long = "a".repeat(MAX_TOKEN_ID_LENGTH + 1);
    let error = ApiToken::generate(too_long, "demo-owner", Utc::now(), TestFixture::metadata());

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "token_id",
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
        BpError::Auth(AuthError::InvalidField { field: "id", .. })
    ));
}

#[test]
fn rejects_missing_vault_value_fields() {
    let error = ApiToken::from_value("token-a", &json!({"owner": "demo-owner"}));

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
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
            Err(BpError::Auth(AuthError::InvalidField {
                field: "secret",
                ..
            }))
        ));
    }
}

#[test]
fn created_at_is_recorded() {
    let before = Utc::now();
    let token = TestFixture::token("token-a");
    let after = Utc::now();

    assert!(token.created_at() >= before && token.created_at() <= after);
}
