use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{TokenExpiry, TokenOptions, TokenScope};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};
use secrecy::ExposeSecret;

#[tokio::test]
async fn finds_tokens_by_owner() {
    let service = TestFixture::service();
    let first = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let second = TestFixture::add_token(&service, "token-b", "owner-b").await;

    let tokens = service.tokens_for_owner("owner-a").await.unwrap();

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id(), first.id());
    assert_ne!(tokens[0].id(), second.id());
}

#[tokio::test]
async fn finds_a_token_by_owner() {
    let service = TestFixture::service();
    let added = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let token = service
        .token_for_owner("owner-a", Utc::now())
        .await
        .unwrap();

    assert_eq!(token.id(), added.id());
}

#[tokio::test]
async fn an_owner_without_a_token_cannot_be_used() {
    let service = TestFixture::service();

    let error = service.token_for_owner("owner-a", Utc::now()).await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[tokio::test]
async fn rejects_an_empty_owner() {
    let service = TestFixture::service();

    let error = service.tokens_for_owner(" ").await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[tokio::test]
async fn signs_order_requests_for_the_owner() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    let signed = service
        .sign_order_request_for_owner("owner-a", ORDER_REQUEST, now, now)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
    assert!(!signed.signature().contains(token.secret().expose_secret()));
    assert!(
        service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.signature(), now)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn chooses_a_deterministic_token_when_an_owner_has_several() {
    let service = TestFixture::service();
    TestFixture::add_token(&service, "token-b", "owner-a").await;
    let first = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let tokens = service.tokens_for_owner("owner-a").await.unwrap();

    assert_eq!(
        tokens.iter().map(|token| token.id()).collect::<Vec<_>>(),
        vec!["token-a", "token-b"]
    );
    assert_eq!(
        service
            .token_for_owner("owner-a", Utc::now())
            .await
            .unwrap()
            .id(),
        first.id()
    );
}

#[tokio::test]
async fn owner_lookups_trim_surrounding_whitespace() {
    let service = TestFixture::service();
    let added = TestFixture::add_token(&service, "token-a", " owner-a ").await;

    let tokens = service.tokens_for_owner(" owner-a ").await.unwrap();

    assert_eq!(added.owner(), "owner-a");
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id(), added.id());
}

#[tokio::test]
async fn signing_for_an_owner_uses_a_token_with_the_required_scope() {
    let service = TestFixture::service();
    let now = Utc::now();
    let read_options = TokenOptions::new(
        TokenExpiry::After(Duration::days(30)),
        [TokenScope::OrdersRead],
    );
    let write_options = TokenOptions::new(
        TokenExpiry::After(Duration::days(30)),
        [TokenScope::OrdersWrite],
    );
    TestFixture::add_token_at(&service, "token-a", "owner-a", now, read_options).await;
    let writer =
        TestFixture::add_token_at(&service, "token-z", "owner-a", now, write_options).await;

    let signed = service
        .sign_order_request_for_owner("owner-a", ORDER_REQUEST, now, now)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), writer.id());
}

#[tokio::test]
async fn signing_for_an_owner_requires_a_token_with_the_required_scope() {
    let service = TestFixture::service();
    let now = Utc::now();
    let read_options = TokenOptions::new(
        TokenExpiry::After(Duration::days(30)),
        [TokenScope::OrdersRead],
    );
    TestFixture::add_token_at(&service, "token-a", "owner-a", now, read_options).await;

    let error = service
        .sign_order_request_for_owner("owner-a", ORDER_REQUEST, now, now)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "scope",
            ..
        }))
    ));
}
