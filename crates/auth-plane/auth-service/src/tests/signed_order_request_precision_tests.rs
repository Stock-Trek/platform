use crate::{OrderRequestContext, SignedOrderRequest};
use chrono::{DateTime, Duration, Timelike};
use platform_errors::{AuthError, BpError};

struct PrecisionFixture;

impl PrecisionFixture {
    fn issued_at() -> DateTime<chrono::Utc> {
        DateTime::from_timestamp(1_700_000_000, 123_456_789)
            .expect("the fixture timestamp is valid")
    }
}

#[test]
fn envelope_timestamps_match_their_serialized_form() {
    let issued_at = PrecisionFixture::issued_at();
    let expires_at = issued_at + Duration::minutes(5);
    let context = OrderRequestContext::new("token-a", issued_at, expires_at, "nonce-a")
        .expect("valid context");

    let signed = SignedOrderRequest::from_context(&context, "signature");
    let decoded = SignedOrderRequest::decode(signed.signature()).expect("valid envelope");

    assert_eq!(signed.issued_at(), decoded.issued_at());
    assert_eq!(signed.expires_at(), decoded.expires_at());
    assert_eq!(signed.issued_at().nanosecond(), 0);
    assert_eq!(signed.expires_at().nanosecond(), 0);
}

#[test]
fn contexts_use_envelope_precision() {
    let issued_at = PrecisionFixture::issued_at();
    let expires_at = issued_at + Duration::minutes(5);
    let context = OrderRequestContext::new("token-a", issued_at, expires_at, "nonce-a")
        .expect("valid context");

    assert_eq!(context.issued_at().nanosecond(), 0);
    assert_eq!(context.expires_at().nanosecond(), 0);
}

#[test]
fn generated_contexts_keep_the_configured_validity() {
    let issued_at = PrecisionFixture::issued_at();
    let context =
        OrderRequestContext::generate("token-a", issued_at, Duration::seconds(1)).unwrap();

    assert_eq!(context.issued_at().timestamp(), issued_at.timestamp());
    assert_eq!(
        (context.expires_at() - context.issued_at()).num_seconds(),
        1
    );
}

#[test]
fn rejects_sub_second_signature_validity() {
    let error = OrderRequestContext::generate(
        "token-a",
        PrecisionFixture::issued_at(),
        Duration::milliseconds(500),
    );

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "expires_at",
            ..
        }))
    ));
}
