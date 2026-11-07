use crate::{OrderRequestContext, SignedOrderRequest};
use chrono::{Duration, Utc};

struct SignedRequestFixture;

impl SignedRequestFixture {
    fn signed() -> SignedOrderRequest {
        let issued_at = chrono::DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap();
        let context = OrderRequestContext::new(
            "token-a",
            issued_at,
            issued_at + Duration::minutes(5),
            "nonce-a",
        )
        .unwrap();
        SignedOrderRequest::from_context(&context, "signature")
    }
}

#[test]
fn round_trips_through_the_signed_envelope() {
    let signed = SignedRequestFixture::signed();

    let decoded = SignedOrderRequest::decode(signed.signature()).unwrap();

    assert_eq!(decoded.token_id(), signed.token_id());
    assert_eq!(decoded.issued_at(), signed.issued_at());
    assert_eq!(decoded.expires_at(), signed.expires_at());
    assert_eq!(decoded.nonce(), signed.nonce());
    assert_eq!(decoded.raw_signature(), "signature");
}

#[test]
fn context_preserves_the_signed_metadata() {
    let signed = SignedRequestFixture::signed();

    let context = signed.context().unwrap();

    assert_eq!(context.token_id(), signed.token_id());
    assert_eq!(context.issued_at(), signed.issued_at());
    assert_eq!(context.expires_at(), signed.expires_at());
    assert_eq!(context.nonce(), signed.nonce());
    assert_eq!(context.version(), "v1");
    assert_eq!(context.algorithm(), "HMAC-SHA256");
}

#[test]
fn rejects_a_malformed_envelope() {
    let error = SignedOrderRequest::decode("not-an-envelope");

    assert!(error.is_err());
}

#[test]
fn rejects_an_envelope_without_a_signature() {
    let signed = SignedRequestFixture::signed();
    let unsigned = signed.signature().rsplit_once('.').unwrap().0;
    let error = SignedOrderRequest::decode(&format!("{unsigned}."));

    assert!(error.is_err());
}

#[test]
fn detects_expiry() {
    let signed = SignedRequestFixture::signed();

    assert!(!signed.is_expired(Utc::now()));
    assert!(signed.is_expired(signed.expires_at()));
}
