use super::support_tests::TestFixture;
use crate::ApiToken;
use platform_errors::{AuthError, BpError};
use serde_json::json;

struct PersistedTokenFixture;

impl PersistedTokenFixture {
    fn value() -> serde_json::Value {
        TestFixture::token("token-a").to_value()
    }
}

#[test]
fn rejects_persisted_owners_with_surrounding_whitespace() {
    let mut value = PersistedTokenFixture::value();
    value["owner"] = json!(" demo-owner ");

    let error = ApiToken::from_value("token-a", &value);

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "owner",
            ..
        }))
    ));
}

#[test]
fn rejects_persisted_owners_with_control_characters() {
    let mut value = PersistedTokenFixture::value();
    value["owner"] = json!("demo\nowner");

    let error = ApiToken::from_value("token-a", &value);

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "owner",
            ..
        }))
    ));
}
