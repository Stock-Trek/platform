use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{InvalidOrderRequest, OrderRequestVerification};
use chrono::Utc;

#[tokio::test]
async fn reports_an_unknown_token_as_invalid() {
    let service = TestFixture::service();
    let verification = service
        .verify_order_request(
            "ghost",
            ORDER_REQUEST,
            "v1.0.0.nonce.token.signature",
            Utc::now(),
        )
        .await
        .unwrap();

    assert_eq!(
        verification.invalid_reason(),
        Some(InvalidOrderRequest::UnknownToken)
    );
}

#[tokio::test]
async fn reports_a_signature_for_another_payload_as_a_mismatch() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();
    let verification = service
        .verify_order_request(token.id(), br#"{"side":"sell"}"#, signed.envelope(), now)
        .await
        .unwrap();

    assert_eq!(
        verification.invalid_reason(),
        Some(InvalidOrderRequest::SignatureMismatch)
    );
}

#[tokio::test]
async fn reports_a_replay_as_invalid() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
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

    assert_eq!(first, OrderRequestVerification::Valid);
    assert_eq!(second.invalid_reason(), Some(InvalidOrderRequest::Replay));
}
