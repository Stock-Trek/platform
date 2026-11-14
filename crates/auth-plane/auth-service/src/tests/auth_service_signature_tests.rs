use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{order_request_context::OrderRequestContext, order_signature::OrderRequestSigner};
use chrono::{DateTime, Utc};
use secrecy::SecretString;

#[tokio::test]
async fn signs_order_requests_with_the_stored_token_secret() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();

    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
    assert!(signed.envelope().starts_with("v1."));
    assert_eq!(signed.signature().len(), 64);
}

#[test]
fn signs_order_requests_with_a_known_hmac_vector() {
    let signer = OrderRequestSigner::new(&SecretString::from("key")).unwrap();
    let context = OrderRequestContext::new(
        "token-a",
        DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        DateTime::from_timestamp(1_700_000_300, 0).unwrap(),
        "nonce-a",
    )
    .unwrap();

    let signature = signer.sign_order_request(&context, b"request").unwrap();

    assert_eq!(
        signature,
        "c0048808899362ac3fcab18f6b44645a7f641d99ded77753c77e11494ea983e2"
    );
}

#[tokio::test]
async fn verifies_a_matching_signature() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();

    assert!(verified.is_valid());
}

#[tokio::test]
async fn rejects_a_tampered_signature() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();
    let tampered = TestFixture::tampered_signature(signed.envelope());

    assert_ne!(tampered, signed.envelope());
    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, &tampered, now)
        .await
        .unwrap();

    assert!(verified.is_invalid());
}

#[tokio::test]
async fn rejects_a_tampered_request() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), br#"{"side":"sell"}"#, signed.envelope(), now)
        .await
        .unwrap();

    assert!(verified.is_invalid());
}
