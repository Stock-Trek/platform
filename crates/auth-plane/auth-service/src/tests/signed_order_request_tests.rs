use super::support_tests::TEST_SIGNATURE;
use crate::{
    OrderRequestContext, SignedOrderRequest, order_request_context::MAX_NONCE_LENGTH,
    signed_order_request::MAX_ENVELOPE_LENGTH,
};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};

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
        SignedOrderRequest::from_context(&context, TEST_SIGNATURE).expect("valid signed request")
    }
}

#[test]
fn round_trips_through_the_signed_envelope() {
    let signed = SignedRequestFixture::signed();

    let decoded = SignedOrderRequest::decode(signed.envelope()).unwrap();

    assert_eq!(decoded.token_id(), signed.token_id());
    assert_eq!(decoded.issued_at(), signed.issued_at());
    assert_eq!(decoded.expires_at(), signed.expires_at());
    assert_eq!(decoded.nonce(), signed.nonce());
    assert_eq!(decoded.signature(), TEST_SIGNATURE);
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
    let unsigned = signed.envelope().rsplit_once('.').unwrap().0;
    let error = SignedOrderRequest::decode(&format!("{unsigned}."));

    assert!(error.is_err());
}

#[test]
fn rejects_non_canonical_timestamp_encodings() {
    let signed = SignedRequestFixture::signed();
    let mut parts = signed.envelope().split('.');
    let version = parts.next().unwrap();
    let issued_at = parts.next().unwrap();
    let rest = parts.collect::<Vec<_>>().join(".");
    let non_canonical = format!("{version}.0{issued_at}.{rest}");

    assert!(SignedOrderRequest::decode(&non_canonical).is_err());
}

#[test]
fn rejects_overlong_nonces() {
    let issued_at = chrono::DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap();
    let nonce = "a".repeat(MAX_NONCE_LENGTH + 1);
    let error = OrderRequestContext::new(
        "token-a",
        issued_at,
        issued_at + Duration::minutes(5),
        nonce,
    );

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "nonce",
            ..
        }))
    ));
}

#[test]
fn rejects_overlong_envelopes() {
    let signature = "a".repeat(MAX_ENVELOPE_LENGTH + 1);
    let error = SignedOrderRequest::decode(&signature);

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "signature",
            ..
        }))
    ));
}

#[test]
fn exposes_the_envelope_and_signature_separately() {
    let signed = SignedRequestFixture::signed();

    assert_eq!(signed.signature(), TEST_SIGNATURE);
    assert!(signed.envelope().starts_with("v1."));
    assert!(signed.envelope().ends_with(&format!(".{TEST_SIGNATURE}")));
}

#[test]
fn from_parts_validates_the_context() {
    let issued_at = chrono::DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap();
    let error = SignedOrderRequest::from_parts(
        "invalid token id",
        issued_at,
        issued_at + Duration::minutes(5),
        "nonce-a",
        "signature",
    );

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        }))
    ));
}

#[test]
fn from_parts_rejects_invalid_raw_signatures() {
    let issued_at = chrono::DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap();
    let invalid = [
        String::new(),
        "signature.with.separators".to_string(),
        "a".repeat(63),
        "a".repeat(65),
        "g".repeat(64),
        "A".repeat(64),
    ];
    for signature in invalid {
        let error = SignedOrderRequest::from_parts(
            "token-a",
            issued_at,
            issued_at + Duration::minutes(5),
            "nonce-a",
            signature,
        );

        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField {
                field: "signature",
                ..
            }))
        ));
    }
}
