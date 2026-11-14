use crate::tests::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{
    ApiToken, MemoryTokenAuditLog, MemoryTokenStore, TokenAuditAction, TokenAuditEvent,
    TokenMutation, TokenRotationTransaction, TokenStore,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use platform_errors::{PlatformError, PlatformResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct FailingMaterializeStore {
    inner: MemoryTokenStore,
    fail_before: AtomicBool,
    fail_after: AtomicBool,
}

impl FailingMaterializeStore {
    fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            fail_before: AtomicBool::new(false),
            fail_after: AtomicBool::new(false),
        }
    }

    fn fail_before_next(&self) {
        self.fail_before.store(true, Ordering::SeqCst);
    }

    fn fail_after_next(&self) {
        self.fail_after.store(true, Ordering::SeqCst);
    }

    fn transient_error() -> PlatformError {
        PlatformError::auth_store(
            "write",
            "rotation-test",
            true,
            "transient materialization failure".to_string(),
        )
    }
}

#[async_trait]
impl TokenStore for FailingMaterializeStore {
    async fn put(&self, token: &ApiToken) -> PlatformResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> PlatformResult<Option<ApiToken>> {
        self.inner.get(id).await
    }

    async fn delete(&self, id: &str) -> PlatformResult<()> {
        self.inner.delete(id).await
    }

    async fn ids(&self) -> PlatformResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> PlatformResult<()> {
        self.inner.update(expected, token).await
    }

    async fn commit(
        &self,
        mutation: TokenMutation,
        event: TokenAuditEvent,
    ) -> PlatformResult<bool> {
        self.inner.commit(mutation, event).await
    }

    async fn append_audit(&self, event: TokenAuditEvent) -> PlatformResult<()> {
        self.inner.append_audit(event).await
    }

    async fn pending_audits(&self) -> PlatformResult<Vec<TokenAuditEvent>> {
        self.inner.pending_audits().await
    }

    async fn acknowledge_audit(&self, event_id: &str) -> PlatformResult<()> {
        self.inner.acknowledge_audit(event_id).await
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> PlatformResult<()> {
        self.inner.commit_rotation(transaction).await
    }

    async fn materialize_rotation(
        &self,
        transaction: &TokenRotationTransaction,
    ) -> PlatformResult<()> {
        if self.fail_before.swap(false, Ordering::SeqCst) {
            return Err(Self::transient_error());
        }
        self.inner.materialize_rotation(transaction).await?;
        if self.fail_after.swap(false, Ordering::SeqCst) {
            return Err(Self::transient_error());
        }
        Ok(())
    }

    async fn pending_rotations(&self) -> PlatformResult<Vec<TokenRotationTransaction>> {
        self.inner.pending_rotations().await
    }

    async fn acknowledge_rotation(&self, transaction_id: &str) -> PlatformResult<()> {
        self.inner.acknowledge_rotation(transaction_id).await
    }
}

struct RotationRetryFixture;

impl RotationRetryFixture {
    fn service() -> (crate::AuthService, Arc<FailingMaterializeStore>) {
        let store = Arc::new(FailingMaterializeStore::new());
        let service = AuthServiceBuilder::new().store(store.clone()).build();
        (service, store)
    }

    fn event_ids(transactions: &[TokenRotationTransaction]) -> Vec<String> {
        transactions[0]
            .events()
            .iter()
            .map(|event| event.event_id().expect("event id").to_string())
            .collect()
    }

    fn successful_actions(events: &[TokenAuditEvent]) -> Vec<TokenAuditAction> {
        events
            .iter()
            .filter(|event| event.success)
            .map(|event| event.action)
            .collect()
    }
}

#[tokio::test]
async fn retrying_after_a_pre_materialization_failure_reuses_the_transaction() {
    let (service, store) = RotationRetryFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    TestFixture::drain(&service, Arc::new(MemoryTokenAuditLog::new(16))).await;
    let now = Utc::now();
    store.fail_before_next();

    service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap_err();
    let first_ids = RotationRetryFixture::event_ids(&store.pending_rotations().await.unwrap());

    service
        .rotate_token_to(original.id(), "token-b", now + Duration::seconds(5))
        .await
        .unwrap();
    let pending = store.pending_rotations().await.unwrap();

    assert_eq!(pending.len(), 1);
    assert_eq!(RotationRetryFixture::event_ids(&pending), first_ids);

    let audit = Arc::new(MemoryTokenAuditLog::new(16));
    TestFixture::drain(&service, audit.clone()).await;
    assert_eq!(
        RotationRetryFixture::successful_actions(&audit.events()),
        vec![TokenAuditAction::Rotated, TokenAuditAction::Issued]
    );
    assert!(store.pending_rotations().await.unwrap().is_empty());
}

#[tokio::test]
async fn recovery_after_materialization_keeps_the_transaction_for_the_relay() {
    let (service, store) = RotationRetryFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    TestFixture::drain(&service, Arc::new(MemoryTokenAuditLog::new(16))).await;
    let now = Utc::now();
    store.fail_after_next();

    let replacement = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();

    assert_eq!(replacement.id(), "token-b");
    assert_eq!(store.pending_rotations().await.unwrap().len(), 1);

    let audit = Arc::new(MemoryTokenAuditLog::new(16));
    TestFixture::drain(&service, audit.clone()).await;

    assert_eq!(
        RotationRetryFixture::successful_actions(&audit.events()),
        vec![TokenAuditAction::Rotated, TokenAuditAction::Issued]
    );
    assert!(store.pending_rotations().await.unwrap().is_empty());
}
