use super::support_tests::{ORDER_REQUEST, TestFixture};
use platform_errors::{AuthError, BpError};
use secrecy::ExposeSecret;

#[tokio::test]
async fn finds_tokens_by_owner() {
    let service = TestFixture::service();
    let first = service.add_token("token-a", "owner-a").await.unwrap();
    let second = service.add_token("token-b", "owner-b").await.unwrap();

    let tokens = service.tokens_for_owner("owner-a").await.unwrap();

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id(), first.id());
    assert_ne!(tokens[0].id(), second.id());
}

#[tokio::test]
async fn finds_a_token_by_owner() {
    let service = TestFixture::service();
    let added = service.add_token("token-a", "owner-a").await.unwrap();

    let token = service.token_for_owner("owner-a").await.unwrap();

    assert_eq!(token.id(), added.id());
}

#[tokio::test]
async fn an_owner_without_a_token_cannot_be_used() {
    let service = TestFixture::service();

    let error = service.token_for_owner("owner-a").await;

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
    let token = service.add_token("token-a", "owner-a").await.unwrap();

    let signed = service
        .sign_order_request_for_owner("owner-a", ORDER_REQUEST)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
    assert!(!signed.signature().contains(token.secret().expose_secret()));
    assert!(
        service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn chooses_a_deterministic_token_when_an_owner_has_several() {
    let service = TestFixture::service();
    service.add_token("token-b", "owner-a").await.unwrap();
    let first = service.add_token("token-a", "owner-a").await.unwrap();

    let tokens = service.tokens_for_owner("owner-a").await.unwrap();

    assert_eq!(
        tokens.iter().map(|token| token.id()).collect::<Vec<_>>(),
        vec!["token-a", "token-b"]
    );
    assert_eq!(
        service.token_for_owner("owner-a").await.unwrap().id(),
        first.id()
    );
}
