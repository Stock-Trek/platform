use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{ApiToken, AuthService, TokenExpiry, TokenOptions, TokenScope};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};

struct ScopeFixture;

impl ScopeFixture {
    async fn add_with_scopes<I, S>(
        service: &AuthService,
        id: &str,
        owner: &str,
        scopes: I,
    ) -> ApiToken
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let options = TokenOptions::new(TokenExpiry::After(Duration::days(30)), scopes);
        service
            .add_token(id, owner, Utc::now(), options)
            .await
            .unwrap()
    }

    fn options<I, S>(scopes: I) -> TokenOptions
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        TokenOptions::new(TokenExpiry::After(Duration::days(30)), scopes)
    }
}

#[tokio::test]
async fn default_tokens_can_read_and_write_orders() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    assert!(token.metadata().has_scope(TokenScope::ORDERS_READ));
    assert!(token.metadata().has_scope(TokenScope::ORDERS_WRITE));
    assert!(
        service
            .authenticate(
                token.id(),
                token.secret(),
                Some(TokenScope::ORDERS_WRITE),
                now
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn read_only_tokens_cannot_sign_orders() {
    let service = TestFixture::service();
    let token =
        ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", [TokenScope::ORDERS_READ])
            .await;
    let now = Utc::now();

    let error = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField { field: "scope", .. })
    ));
}

#[tokio::test]
async fn wildcard_scopes_grant_every_permission() {
    let service = TestFixture::service();
    let token =
        ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", [TokenScope::ALL]).await;
    let now = Utc::now();

    assert!(token.metadata().has_scope(TokenScope::ORDERS_WRITE));
    assert!(token.metadata().has_scope(TokenScope::ADMIN));
    assert!(
        service
            .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn namespace_wildcards_match_their_namespace_only() {
    let service = TestFixture::service();
    let token = ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", ["orders:*"]).await;

    assert!(token.metadata().has_scope(TokenScope::ORDERS_READ));
    assert!(token.metadata().has_scope(TokenScope::ORDERS_WRITE));
    assert!(!token.metadata().has_scope(TokenScope::TOKENS_WRITE));
}

#[tokio::test]
async fn invalid_scopes_are_rejected() {
    let service = TestFixture::service();

    for scope in [
        "",
        "orders write",
        "orders/**",
        "orders:*:extra",
        ":",
        "orders::read",
    ] {
        let error = service
            .add_token(
                "token-a",
                "owner-a",
                Utc::now(),
                ScopeFixture::options([scope]),
            )
            .await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField {
                field: "scope",
                ..
            }))
        ));
    }
}

#[tokio::test]
async fn scopes_survive_a_vault_value_round_trip() {
    let service = TestFixture::service();
    let token =
        ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", [TokenScope::ORDERS_READ])
            .await;

    let restored = crate::ApiToken::from_value(token.id(), &token.to_value()).unwrap();

    assert_eq!(restored.metadata().scopes(), token.metadata().scopes());
    assert!(restored.metadata().has_scope(TokenScope::ORDERS_READ));
    assert!(!restored.metadata().has_scope(TokenScope::ORDERS_WRITE));
}

#[tokio::test]
async fn authorization_rejects_tokens_without_the_required_scope() {
    let service = TestFixture::service();
    let token =
        ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", [TokenScope::ORDERS_READ])
            .await;

    let error = service
        .authenticate(
            token.id(),
            token.secret(),
            Some(TokenScope::ORDERS_WRITE),
            Utc::now(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField { field: "scope", .. })
    ));
}
