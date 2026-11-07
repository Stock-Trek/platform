use super::support_tests::{ORDER_REQUEST, TestFixture};
use platform_errors::{AuthError, BpError};

#[tokio::test]
async fn signing_requires_the_token_secret() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();

    let error = service
        .sign_order_request(token.id(), "wrong-secret", ORDER_REQUEST)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[tokio::test]
async fn signing_an_unknown_token_fails() {
    let service = TestFixture::service();

    let error = service
        .sign_order_request("ghost", "secret", ORDER_REQUEST)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn signing_after_deletion_fails() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    service.delete_token(token.id()).await.unwrap();

    let error = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));

    let error = service
        .verify_order_request(token.id(), ORDER_REQUEST, "v1.0.0.nonce.token.signature")
        .await;
    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn rejects_path_traversal_token_ids_before_signing() {
    let service = TestFixture::service();

    for id in ["../metadata", "../../../sys/health", "not/a/path", ""] {
        let error = service
            .sign_order_request(id, "secret", ORDER_REQUEST)
            .await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service
            .verify_order_request(id, ORDER_REQUEST, "v1.0.0.nonce.token.signature")
            .await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
    }
}
