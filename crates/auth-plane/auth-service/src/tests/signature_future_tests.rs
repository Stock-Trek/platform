use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{
    ApiToken, MemoryTokenAuditLog, OrderRequestContext, OrderRequestSigner, SignedOrderRequest,
    TokenAuditAction,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, BpError};
use std::sync::Arc;

struct FutureSignatureFixture;

impl FutureSignatureFixture {
    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    fn signed(token: &ApiToken, now: DateTime<Utc>) -> SignedOrderRequest {
        let issued_at = now + Duration::minutes(10);
        let context = OrderRequestContext::new(
            token.id(),
            issued_at,
            issued_at + Duration::minutes(5),
            "nonce-a",
        )
        .expect("valid context");
        let signer = OrderRequestSigner::new(token.secret()).expect("valid signer");
        let signature = signer
            .sign_order_request(&context, ORDER_REQUEST)
            .expect("signing succeeds");
        SignedOrderRequest::from_context(&context, signature).expect("valid signed request")
    }
}

#[tokio::test]
async fn signing_rejects_a_signature_issued_in_the_future() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = FutureSignatureFixture::now();

    let error = service
        .sign_order_request(
            token.id(),
            token.secret(),
            ORDER_REQUEST,
            now + Duration::minutes(10),
            now,
        )
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "issued_at",
            ..
        }))
    ));
}

#[tokio::test]
async fn rejects_a_signature_issued_in_the_future() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = FutureSignatureFixture::now();
    let signed = FutureSignatureFixture::signed(&token, now);

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();

    assert!(verified.is_invalid());
}

#[tokio::test]
async fn records_the_reason_a_future_signature_is_rejected() {
    let audit = Arc::new(MemoryTokenAuditLog::new(4096));
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = FutureSignatureFixture::now();
    let signed = FutureSignatureFixture::signed(&token, now);

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();

    assert!(verified.is_invalid());
    TestFixture::drain(&service, audit.clone()).await;
    let rejection = audit
        .events()
        .into_iter()
        .find(|event| event.action == TokenAuditAction::SignatureRejected)
        .expect("a rejection event should be recorded");
    assert_eq!(
        rejection.detail.as_deref(),
        Some("signature issued in the future")
    );
}
