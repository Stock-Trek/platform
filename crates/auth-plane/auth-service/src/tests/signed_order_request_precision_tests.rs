use crate::{OrderRequestContext, SignedOrderRequest};
use chrono::{DateTime, Duration, Timelike};

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
