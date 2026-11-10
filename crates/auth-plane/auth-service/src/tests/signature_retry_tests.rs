use super::support_tests::{AuthServiceBuilder, ORDER_REQUEST, TestFixture};
use crate::{
    ApiToken, AuthService, MemoryTokenStore, TokenAuditAction, TokenAuditEvent, TokenAuditLog,
    TokenMutation, TokenRotationTransaction, TokenStore,
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

struct FailingStore {
    inner: MemoryTokenStore,
    fail_next_update: AtomicBool,
}

impl FailingStore {
    fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            fail_next_update: AtomicBool::new(false),
        }
    }

    fn fail_next_update(&self) {
        self.fail_next_update.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl TokenStore for FailingStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.inner.get(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        self.inner.delete(id).await
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        if self.fail_next_update.swap(false, Ordering::SeqCst) {
            return Err(BpError::auth_store(
                "update",
                token.id(),
                true,
                "transient failure".to_string(),
            ));
        }
        self.inner.update(expected, token).await
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        self.inner.commit_rotation(transaction).await
    }

    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent) -> BpResult<bool> {
        match mutation {
            TokenMutation::Create(token) => {
                self.inner.commit(TokenMutation::Create(token), event).await
            }
            TokenMutation::Update { expected, token } => {
                self.update(&expected, &token).await?;
                self.inner.append_audit(event).await?;
                Ok(true)
            }
            TokenMutation::Delete { expected } => {
                if self.delete_if(&expected).await? {
                    self.inner.append_audit(event).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    async fn append_audit(&self, event: TokenAuditEvent) -> BpResult<()> {
        self.inner.append_audit(event).await
    }

    async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        self.inner.pending_audits().await
    }

    async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        self.inner.acknowledge_audit(event_id).await
    }
}

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

struct SignatureRetryFixture;

impl SignatureRetryFixture {
    fn service() -> (AuthService, Arc<FailingStore>) {
        let store = Arc::new(FailingStore::new());
        let service = AuthServiceBuilder::new().store(store.clone()).build();
        (service, store)
    }
}

#[tokio::test]
async fn retries_audit_delivery_without_rejecting_the_signature() {
    let audit = Arc::new(FailingSignatureVerifiedAuditLog::new());
    let service = TestFixture::service_with_audit(audit.clone());
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("verification succeeds despite audit delivery failure");
    assert!(verified);
    assert!(
        !audit
            .events()
            .iter()
            .any(|event| event.action == TokenAuditAction::SignatureVerified)
    );

    let drained = service
        .relay_audit_outbox()
        .await
        .expect("audit retry succeeds");
    assert_eq!(drained, 1);
    assert!(
        audit
            .events()
            .iter()
            .any(|event| event.action == TokenAuditAction::SignatureVerified)
    );
}

#[tokio::test]
async fn retries_a_signature_after_the_token_store_update_fails() {
    let (service, store) = SignatureRetryFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    store.fail_next_update();
    let error = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await;
    assert!(error.is_err());

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("retried verification");
    assert!(verified);
}
