use crate::OrderRequestSigner;
use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BuildError};
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
        .verify_order_request(second.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();

    assert!(verified.is_invalid());
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
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();
    let second = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();

    assert!(first.is_valid());
    assert!(second.is_invalid());
}

#[test]
fn rejects_blank_signing_secrets() {
    for secret in ["", " ", "\t"] {
        let error = OrderRequestSigner::new(&SecretString::from(secret));

        assert!(matches!(
            error,
            Err(BuildError::Auth(AuthError::InvalidField {
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
    let issued_at = now - Duration::minutes(4);
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
        .verify_order_request(
            token.id(),
            ORDER_REQUEST,
            signed.envelope(),
            now + Duration::minutes(2),
        )
        .await
        .unwrap();

    assert!(verified.is_invalid());
}

#[tokio::test]
async fn signing_rejects_an_already_expired_request() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let issued_at = now - Duration::minutes(10);

    let error = service
        .sign_order_request(
            token.id(),
            token.secret().expose_secret(),
            ORDER_REQUEST,
            issued_at,
            now,
        )
        .await;

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "expires_at",
            ..
        }))
    ));
}
