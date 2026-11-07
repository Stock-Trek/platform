use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::OrderRequestSigner;
use chrono::{Duration as ChronoDuration, Utc};
use platform_errors::{AuthError, BpError};
use secrecy::{ExposeSecret, SecretString};

#[tokio::test]
async fn rejects_a_signature_from_another_token() {
    let service = TestFixture::service();
    let first = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let second = TestFixture::add_token(&service, "token-b", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(first.id(), first.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(second.id(), ORDER_REQUEST, signed.signature(), now)
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn rejects_a_replayed_signature() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    let first = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature(), now)
        .await
        .unwrap();
    let second = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature(), now)
        .await
        .unwrap();

    assert!(first);
    assert!(!second);
}

#[test]
fn rejects_blank_signing_secrets() {
    for secret in ["", " ", "\t"] {
        let error = OrderRequestSigner::new(&SecretString::from(secret));

        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField {
                field: "secret",
                ..
            }))
        ));
    }
}

#[tokio::test]
async fn rejects_an_expired_signature() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let issued_at = now - ChronoDuration::minutes(10);
    let signed = service
        .sign_order_request(
            token.id(),
            token.secret().expose_secret(),
            ORDER_REQUEST,
            issued_at,
            now,
        )
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature(), now)
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn rejects_a_signature_issued_in_the_future() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let issued_at = now + ChronoDuration::minutes(10);
    let signed = service
        .sign_order_request(
            token.id(),
            token.secret().expose_secret(),
            ORDER_REQUEST,
            issued_at,
            now,
        )
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature(), now)
        .await
        .unwrap();

    assert!(!verified);
}
