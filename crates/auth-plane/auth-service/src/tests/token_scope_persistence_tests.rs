use crate::tests::support_tests::TestFixture;
use crate::{ApiToken, AuthService, TokenExpiry, TokenOptions, TokenScope};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BuildError};
use serde_json::json;

struct ScopePersistenceFixture;

impl ScopePersistenceFixture {
    async fn add_read_only_token(service: &AuthService) -> ApiToken {
        let options = TokenOptions::new(
            TokenExpiry::After(Duration::days(30)),
            [TokenScope::OrdersRead],
        );
        let now = Utc::now();
        service
            .add_token("token-a", "owner-a", now, options, now)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn scopes_survive_a_vault_value_round_trip() {
    let service = TestFixture::service();
    let token = ScopePersistenceFixture::add_read_only_token(&service).await;

    let restored = ApiToken::from_value(token.id(), &token.to_value()).unwrap();

    assert_eq!(restored.metadata().scopes(), token.metadata().scopes());
    assert!(restored.metadata().has_scope(TokenScope::OrdersRead));
    assert!(!restored.metadata().has_scope(TokenScope::OrdersWrite));
}

#[test]
fn rejects_empty_scopes_when_restored() {
    let mut value = TestFixture::token("token-a").to_value();
    value["scopes"] = json!([]);

    let error = ApiToken::from_value("token-a", &value).unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::InvalidField {
            field: "scopes",
            ..
        })
    ));
}

#[test]
fn invalid_scopes_are_rejected_when_restored() {
    let mut value = TestFixture::token("token-a").to_value();
    value["scopes"] = json!(["orders:read", "orders/**"]);

    let error = ApiToken::from_value("token-a", &value).unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::InvalidField {
            field: "scopes",
            ..
        })
    ));
}

#[tokio::test]
async fn authorization_rejects_tokens_without_the_required_scope() {
    let service = TestFixture::service();
    let token = ScopePersistenceFixture::add_read_only_token(&service).await;

    let error = service
        .authenticate(
            token.id(),
            token.secret(),
            Some(TokenScope::OrdersWrite),
            Utc::now(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::MissingScope {
            ref token_id,
            ref scope,
        }) if token_id == token.id() && scope == "orders:write"
    ));
}

#[tokio::test]
async fn authorization_accepts_tokens_with_the_required_scope() {
    let service = TestFixture::service();
    let token = ScopePersistenceFixture::add_read_only_token(&service).await;

    let authenticated = service
        .authenticate(
            token.id(),
            token.secret(),
            Some(TokenScope::OrdersRead),
            Utc::now(),
        )
        .await
        .unwrap();

    assert_eq!(authenticated.id(), token.id());
    assert_eq!(authenticated.owner(), token.owner());
}
