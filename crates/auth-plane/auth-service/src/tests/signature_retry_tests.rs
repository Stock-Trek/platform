use super::support_tests::{ORDER_REQUEST, TestFixture};
use super::token_usage_queue_fixture_tests::ToggleTokenUsageQueue;
use crate::{TokenAuditAction, TokenAuditEvent, TokenAuditLog, TokenAuditRelay};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

struct FailingSignatureVerifiedAuditLog {
    failed: AtomicBool,
    events: Mutex<Vec<TokenAuditEvent>>,
}

impl FailingSignatureVerifiedAuditLog {
    fn new() -> Self {
        Self {
            failed: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
        }
    }

    fn events(&self) -> Vec<TokenAuditEvent> {
        self.events.lock().expect("audit events lock").clone()
    }
}

#[async_trait]
impl TokenAuditLog for FailingSignatureVerifiedAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        if event.action == TokenAuditAction::SignatureVerified
            && !self.failed.swap(true, Ordering::SeqCst)
        {
            return Err(BpError::auth_invalid_field("audit", "queue unavailable"));
        }
        self.events.lock().expect("audit events lock").push(event);
        Ok(())
    }
}

#[tokio::test]
async fn retries_audit_delivery_without_rejecting_the_signature() {
    let audit = Arc::new(FailingSignatureVerifiedAuditLog::new());
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    TestFixture::drain(&service, audit.clone()).await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("verification succeeds despite audit delivery failure");
    assert!(verified.is_valid());
    assert!(
        !audit
            .events()
            .iter()
            .any(|event| event.action == TokenAuditAction::SignatureVerified)
    );

    let relay = TokenAuditRelay::new(service.store.clone(), audit.clone());
    assert!(relay.drain().await.is_err());
    assert!(
        !audit
            .events()
            .iter()
            .any(|event| event.action == TokenAuditAction::SignatureVerified)
    );
    assert_eq!(relay.drain().await.expect("audit retry succeeds"), 1);
    assert!(
        audit
            .events()
            .iter()
            .any(|event| event.action == TokenAuditAction::SignatureVerified)
    );
}

#[tokio::test]
async fn retries_a_signature_after_the_usage_queue_fails() {
    let queue = Arc::new(ToggleTokenUsageQueue::new());
    let service = TestFixture::service_with_usage(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    queue.fail();
    let verification = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("queue failure is classified");
    assert!(verification.is_retryable());

    queue.succeed();
    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("retried verification");
    assert!(verified.is_valid());
}
