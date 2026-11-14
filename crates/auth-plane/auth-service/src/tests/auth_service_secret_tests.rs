use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use chrono::Utc;
use platform_errors::{AuthError, PlatformError};

#[tokio::test]
async fn signing_requires_the_token_secret() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();

    let error = service
        .sign_order_request(token.id(), "wrong-secret", ORDER_REQUEST, now, now)
        .await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::InvalidSecret { .. }))
    ));
}

#[tokio::test]
async fn signing_an_unknown_token_fails() {
    let service = TestFixture::service();
    let now = Utc::now();

    let error = service
        .sign_order_request("ghost", "secret", ORDER_REQUEST, now, now)
        .await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn signing_after_deletion_fails() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    service.delete_token(token.id(), Utc::now()).await.unwrap();
    let now = Utc::now();

    let error = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::UnknownToken { .. }))
    ));

    let verified = service
        .verify_order_request(
            token.id(),
            ORDER_REQUEST,
            "v1.0.0.nonce.token.signature",
            now,
        )
        .await
        .unwrap();
    assert!(verified.is_invalid());
}

#[tokio::test]
async fn rejects_path_traversal_token_ids_before_signing() {
    let service = TestFixture::service();

    for id in ["../metadata", "../../../sys/health", "not/a/path", ""] {
        let now = Utc::now();
        let error = service
            .sign_order_request(id, "secret", ORDER_REQUEST, now, now)
            .await;
        assert!(matches!(
            error,
            Err(PlatformError::Auth(AuthError::InvalidField { .. }))
        ));
    }
}

#[tokio::test]
async fn treats_malformed_token_ids_as_verification_failures() {
    let service = TestFixture::service();

    for id in ["../metadata", "../../../sys/health", "not/a/path", ""] {
        let now = Utc::now();
        let verified = service
            .verify_order_request(id, ORDER_REQUEST, "v1.0.0.nonce.token.signature", now)
            .await
            .unwrap();
        assert!(verified.is_invalid());
    }
}
