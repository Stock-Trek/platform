use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{
    OrderRequestContext, OrderRequestSigner, SignedOrderRequest, TokenExpiry, TokenOptions,
    TokenScope,
};
use chrono::{DateTime, Duration, Utc};

struct ClockSkewFixture;

impl ClockSkewFixture {
    fn now() -> DateTime<Utc> {
        DateTime::<Utc>::MAX_UTC - Duration::seconds(1)
    }

    fn expires_at() -> DateTime<Utc> {
        DateTime::<Utc>::MAX_UTC
    }
}

#[tokio::test]
async fn verification_handles_clock_skew_at_the_timestamp_limit() {
    let service = TestFixture::service();
    let options = TokenOptions::new(
        TokenExpiry::At(ClockSkewFixture::expires_at()),
        TokenScope::DEFAULT,
    );
    let token =
        TestFixture::add_token_at(&service, "token-a", "owner-a", Utc::now(), options).await;
    let now = ClockSkewFixture::now();
    let context =
        OrderRequestContext::new(token.id(), now, ClockSkewFixture::expires_at(), "nonce-a")
            .expect("valid context");
    let signer = OrderRequestSigner::new(token.secret()).expect("valid signer");
    let signature = signer
        .sign_order_request(&context, ORDER_REQUEST)
        .expect("signing succeeds");
    let signed = SignedOrderRequest::from_context(&context, signature);

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature(), now)
        .await
        .expect("verification succeeds");

    assert!(verified);
}
