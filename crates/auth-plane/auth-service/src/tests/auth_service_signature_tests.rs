use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::OrderRequestSigner;
use chrono::{Duration as ChronoDuration, Utc};
use secrecy::{ExposeSecret, SecretString};

#[tokio::test]
async fn signs_order_requests_with_the_stored_token_secret() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();

    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
    assert!(signed.signature().starts_with("v1."));
    assert_eq!(signed.raw_signature().len(), 64);
}

#[tokio::test]
async fn signs_order_requests_with_the_secret_after_the_request() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();

    let signed = service
        .sign_order_request(token.id(), ORDER_REQUEST, token.secret())
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
}

#[tokio::test]
async fn signs_the_rfc4231_reference_vector() {
    let signer = OrderRequestSigner::new(&SecretString::from("key")).unwrap();

    let signature = signer
        .sign(b"The quick brown fox jumps over the lazy dog")
        .unwrap();

    assert_eq!(
        signature,
        "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
    );
}

#[tokio::test]
async fn verifies_a_matching_signature() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(verified);
}

#[tokio::test]
async fn rejects_a_tampered_signature() {
    for _ in 0..32 {
        let service = TestFixture::service();
        let token = service.add_token("token-a", "demo-owner").await.unwrap();
        let signed = service
            .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
            .await
            .unwrap();
        let tampered = TestFixture::tampered_signature(signed.signature());

        assert_ne!(tampered, signed.signature());
        let verified = service
            .verify_order_request(token.id(), ORDER_REQUEST, &tampered)
            .await
            .unwrap();

        assert!(!verified);
    }
}

#[tokio::test]
async fn rejects_a_tampered_request() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), br#"{"side":"sell"}"#, signed.signature())
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn rejects_a_signature_from_another_token() {
    let service = TestFixture::service();
    let first = service.add_token("token-a", "demo-owner").await.unwrap();
    let second = service.add_token("token-b", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(first.id(), first.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(second.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn rejects_a_replayed_signature() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let first = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();
    let second = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(first);
    assert!(!second);
}

#[tokio::test]
async fn rejects_an_expired_signature() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let issued_at = Utc::now() - ChronoDuration::minutes(10);
    let signed = service
        .sign_order_request_at(
            token.id(),
            token.secret().expose_secret(),
            ORDER_REQUEST,
            issued_at,
        )
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn rejects_a_signature_issued_in_the_future() {
    let service = TestFixture::service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let issued_at = Utc::now() + ChronoDuration::minutes(10);
    let signed = service
        .sign_order_request_at(
            token.id(),
            token.secret().expose_secret(),
            ORDER_REQUEST,
            issued_at,
        )
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(!verified);
}
