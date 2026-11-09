use super::support_tests::TestFixture;
use crate::{ApiToken, AuthService, MemoryReplayStore, MemoryTokenStore, TokenStore};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{AuthError, BpError, BpResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct FailingRotationStore {
    inner: MemoryTokenStore,
    fail_update: AtomicBool,
    fail_delete: AtomicBool,
}

impl FailingRotationStore {
    fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            fail_update: AtomicBool::new(false),
            fail_delete: AtomicBool::new(false),
        }
    }

    fn fail_update(&self) {
        self.fail_update.store(true, Ordering::SeqCst);
    }

    fn fail_delete(&self) {
        self.fail_delete.store(true, Ordering::SeqCst);
    }

    fn allow_update(&self) {
        self.fail_update.store(false, Ordering::SeqCst);
    }

    fn allow_delete(&self) {
        self.fail_delete.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl TokenStore for FailingRotationStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.inner.get(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(BpError::auth_store(
                "delete",
                id,
                true,
                "delete failed".to_string(),
            ));
        }
        self.inner.delete(id).await
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        if self.fail_update.load(Ordering::SeqCst) {
            return Err(BpError::auth_store(
                "update",
                token.id(),
                true,
                "update failed".to_string(),
            ));
        }
        self.inner.update(expected, token).await
    }
}

struct RotationRollbackFixture;

impl RotationRollbackFixture {
    fn service() -> (AuthService, Arc<FailingRotationStore>) {
        let store = Arc::new(FailingRotationStore::new());
        let service = AuthService::new(
            store.clone(),
            TestFixture::config(),
            Arc::new(MemoryReplayStore::default()),
        )
        .expect("valid service");
        (service, store)
    }
}

#[tokio::test]
async fn failed_rotation_removes_the_replacement() {
    let (service, store) = RotationRollbackFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    store.fail_update();

    let error = service
        .rotate_token_to(original.id(), "token-b", Utc::now())
        .await
        .unwrap_err();

    assert!(matches!(error, BpError::Auth(AuthError::Store { .. })));
    assert!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
    assert!(matches!(
        service.token("token-b").await,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn failed_rotation_reports_a_failed_rollback() {
    let (service, store) = RotationRollbackFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    store.fail_update();
    store.fail_delete();

    let error = service
        .rotate_token_to(original.id(), "token-b", Utc::now())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Store {
            operation: "rollback",
            ..
        })
    ));
    assert!(service.token("token-b").await.is_ok());
    assert!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
}

#[tokio::test]
async fn failed_rotation_can_be_retried_after_the_replacement_was_written() {
    let (service, store) = RotationRollbackFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    store.fail_update();
    store.fail_delete();
    let now = Utc::now();

    service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap_err();

    store.allow_update();
    store.allow_delete();
    let replacement = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();

    assert_eq!(replacement.id(), "token-b");
    assert_eq!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by(),
        Some(replacement.id())
    );
}

#[tokio::test]
async fn retrying_a_completed_rotation_returns_the_same_replacement() {
    let (service, _) = RotationRollbackFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    let first = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();
    let second = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();

    assert_eq!(first.id(), second.id());
    assert_eq!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by(),
        Some(second.id())
    );
}
