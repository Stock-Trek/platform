use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{ApiToken, AuthService, TokenExpiry, TokenOptions, TokenScope};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BuildError};

struct ScopeFixture;

impl ScopeFixture {
    async fn add_with_scopes<I, S>(
        service: &AuthService,
        id: &str,
        owner: &str,
        scopes: I,
    ) -> ApiToken
    where
        S: std::borrow::Borrow<TokenScope>,
        I: IntoIterator<Item = S>,
    {
        let options = TokenOptions::new(TokenExpiry::After(Duration::days(30)), scopes);
        let now = Utc::now();
        service
            .add_token(id, owner, now, options, now)
            .await
            .unwrap()
    }
}

#[test]
fn scopes_have_stable_string_representations() {
    let expected = [
        (TokenScope::All, "*"),
        (TokenScope::OrdersRead, "orders:read"),
        (TokenScope::OrdersWrite, "orders:write"),
        (TokenScope::OrdersAll, "orders:*"),
        (TokenScope::TokensRead, "tokens:read"),
        (TokenScope::TokensWrite, "tokens:write"),
        (TokenScope::TokensAll, "tokens:*"),
        (TokenScope::Admin, "admin"),
    ];

    for (scope, value) in expected {
        assert_eq!(scope.to_string(), value);
        assert_eq!(value.parse::<TokenScope>().unwrap(), scope);
    }
}

#[tokio::test]
async fn default_tokens_can_read_and_write_orders() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    assert!(token.metadata().has_scope(TokenScope::OrdersRead));
    assert!(token.metadata().has_scope(TokenScope::OrdersWrite));
    assert!(
        service
            .authenticate(
                token.id(),
                token.secret(),
                Some(TokenScope::OrdersWrite),
                now
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn rejects_tokens_without_scopes() {
    let service = TestFixture::service();
    let options = TokenOptions::new(
        TokenExpiry::After(Duration::days(30)),
        Vec::<TokenScope>::new(),
    );

    let now = Utc::now();
    let error = service
        .add_token("token-a", "owner-a", now, options, now)
        .await;

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "scopes",
            ..
        }))
    ));
}

#[tokio::test]
async fn read_only_tokens_cannot_sign_orders() {
    let service = TestFixture::service();
    let token =
        ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", [TokenScope::OrdersRead])
            .await;
    let now = Utc::now();

    let error = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::MissingScope { .. })
    ));
}

#[tokio::test]
async fn wildcard_scopes_grant_every_permission() {
    let service = TestFixture::service();
    let token =
        ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", [TokenScope::All]).await;
    let now = Utc::now();

    assert!(token.metadata().has_scope(TokenScope::OrdersWrite));
    assert!(token.metadata().has_scope(TokenScope::Admin));
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
    let token =
        ScopeFixture::add_with_scopes(&service, "token-a", "owner-a", [TokenScope::OrdersAll])
            .await;

    assert!(token.metadata().has_scope(TokenScope::OrdersRead));
    assert!(token.metadata().has_scope(TokenScope::OrdersWrite));
    assert!(!token.metadata().has_scope(TokenScope::TokensWrite));
}

#[test]
fn invalid_scopes_are_rejected_when_parsed() {
    for scope in [
        "",
        "orders write",
        "orders/**",
        "orders:*:extra",
        ":",
        "orders::read",
    ] {
        assert!(scope.parse::<TokenScope>().is_err());
    }
}
