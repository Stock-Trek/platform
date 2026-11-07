use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::TokenScope;
use platform_errors::{AuthError, BpError};

#[tokio::test]
async fn default_tokens_can_read_and_write_orders() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "owner-a").await.unwrap();

    assert!(token.has_scope(TokenScope::ORDERS_READ));
    assert!(token.has_scope(TokenScope::ORDERS_WRITE));
    assert!(
        service
            .authenticate_with_scope(token.id(), token.secret(), TokenScope::ORDERS_WRITE)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn read_only_tokens_cannot_sign_orders() {
    let service = TestFixture::service();
    let token = service
        .add_token_with_scopes("token-a", "owner-a", [TokenScope::ORDERS_READ])
        .await
        .unwrap();

    let error = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
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
    let token = service
        .add_token_with_scopes("token-a", "owner-a", [TokenScope::ALL])
        .await
        .unwrap();

    assert!(token.has_scope(TokenScope::ORDERS_WRITE));
    assert!(token.has_scope(TokenScope::ADMIN));
    assert!(
        service
            .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn namespace_wildcards_match_their_namespace_only() {
    let service = TestFixture::service();
    let token = service
        .add_token_with_scopes("token-a", "owner-a", ["orders:*"])
        .await
        .unwrap();

    assert!(token.has_scope(TokenScope::ORDERS_READ));
    assert!(token.has_scope(TokenScope::ORDERS_WRITE));
    assert!(!token.has_scope(TokenScope::TOKENS_WRITE));
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
            .add_token_with_scopes("token-a", "owner-a", [scope])
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
    let token = service
        .add_token_with_scopes("token-a", "owner-a", [TokenScope::ORDERS_READ])
        .await
        .unwrap();

    let restored = crate::ApiToken::from_value(token.id(), &token.to_value()).unwrap();

    assert_eq!(restored.scopes(), token.scopes());
    assert!(restored.has_scope(TokenScope::ORDERS_READ));
    assert!(!restored.has_scope(TokenScope::ORDERS_WRITE));
}

#[tokio::test]
async fn authorization_rejects_tokens_without_the_required_scope() {
    let service = TestFixture::service();
    let token = service
        .add_token_with_scopes("token-a", "owner-a", [TokenScope::ORDERS_READ])
        .await
        .unwrap();

    let error = service
        .authorize(token.id(), token.secret(), TokenScope::ORDERS_WRITE)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField { field: "scope", .. })
    ));
}
