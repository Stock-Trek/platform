use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{OrderRequestContext, OrderRequestSigner, SignedOrderRequest};
use chrono::{Duration, Utc};

struct SignaturePolicyFixture;

impl SignaturePolicyFixture {
    async fn verify_with_validity(validity: Duration, now: chrono::DateTime<Utc>) -> (bool, bool) {
        let service = TestFixture::service();
        let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
        let now = now.max(token.created_at());
        let context = OrderRequestContext::new(token.id(), now, now + validity, "nonce-a")
            .expect("valid context");
        let signer = OrderRequestSigner::new(token.secret()).expect("valid signer");
        let signature = signer
            .sign_order_request(&context, ORDER_REQUEST)
            .expect("signing succeeds");
        let signed =
            SignedOrderRequest::from_context(&context, signature).expect("valid signed request");
        let first = service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("verification succeeds");
        let second = service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("verification succeeds");
        (first, second)
    }
}

#[tokio::test]
async fn accepts_signatures_at_the_configured_validity() {
    let (verified, replayed) =
        SignaturePolicyFixture::verify_with_validity(Duration::minutes(5), Utc::now()).await;

    assert!(verified);
    assert!(!replayed);
}

#[tokio::test]
async fn rejects_signatures_that_outlive_the_configured_validity() {
    let (verified, _) =
        SignaturePolicyFixture::verify_with_validity(Duration::minutes(10), Utc::now()).await;

    assert!(!verified);
}
