use super::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{ApiToken, AuthService, MemoryTokenStore, RevocationReason, RevokedBy, TokenStore};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct FailingStore {
    inner: MemoryTokenStore,
    fail_on_id: String,
    fail_pending: AtomicBool,
}

impl FailingStore {
    fn new(fail_on_id: &str) -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            fail_on_id: fail_on_id.to_string(),
            fail_pending: AtomicBool::new(true),
        }
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
        if token.id() == self.fail_on_id && self.fail_pending.swap(false, Ordering::SeqCst) {
            return Err(BpError::auth_store(
                "update",
                token.id(),
                true,
                "injected store failure",
            ));
        }
        self.inner.update(expected, token).await
    }
}

struct RacingStore {
    inner: MemoryTokenStore,
    race_on_id: String,
    race_pending: AtomicBool,
}

impl RacingStore {
    fn new(race_on_id: &str) -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            race_on_id: race_on_id.to_string(),
            race_pending: AtomicBool::new(true),
        }
    }

    async fn revoke_concurrently(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        let Some(mut current) = self.inner.get(token.id()).await? else {
            return Err(BpError::unknown_token(token.id()));
        };
        current.metadata.revoke(
            Utc::now(),
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
        );
        self.inner.update(expected, &current).await
    }
}

#[async_trait]
impl TokenStore for RacingStore {
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
        if token.id() == self.race_on_id && self.race_pending.swap(false, Ordering::SeqCst) {
            self.revoke_concurrently(expected, token).await?;
        }
        self.inner.update(expected, token).await
    }
}

struct OwnerRevocationFixture;

impl OwnerRevocationFixture {
    fn service(store: Arc<dyn TokenStore>) -> AuthService {
        AuthServiceBuilder::new().store(store).build()
    }

    async fn add_three_tokens(service: &AuthService) {
        TestFixture::add_token(service, "token-a", "owner-a").await;
        TestFixture::add_token(service, "token-b", "owner-a").await;
        TestFixture::add_token(service, "token-c", "owner-a").await;
    }

    async fn assert_revoked(service: &AuthService, id: &str) -> bool {
        service.token(id).await.unwrap().metadata().is_revoked()
    }

    async fn revoke_owner(service: &AuthService) -> BpResult<Vec<ApiToken>> {
        service
            .revoke_tokens_for_owner(
                "owner-a",
                RevocationReason::NoLongerNeeded,
                RevokedBy::Owner,
                Utc::now(),
            )
            .await
    }
}

#[tokio::test]
async fn owner_revocation_continues_after_a_store_failure_and_can_be_retried() {
    let store = Arc::new(FailingStore::new("token-b"));
    let service = OwnerRevocationFixture::service(store);
    OwnerRevocationFixture::add_three_tokens(&service).await;

    let error = OwnerRevocationFixture::revoke_owner(&service)
        .await
        .unwrap_err();

    assert!(error.retryability().is_retryable());
    assert!(OwnerRevocationFixture::assert_revoked(&service, "token-a").await);
    assert!(!OwnerRevocationFixture::assert_revoked(&service, "token-b").await);
    assert!(OwnerRevocationFixture::assert_revoked(&service, "token-c").await);

    let retried = OwnerRevocationFixture::revoke_owner(&service)
        .await
        .unwrap();

    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].id(), "token-b");
    for id in ["token-a", "token-b", "token-c"] {
        assert!(OwnerRevocationFixture::assert_revoked(&service, id).await);
    }
}

#[tokio::test]
async fn owner_revocation_ignores_a_concurrent_revocation() {
    let store = Arc::new(RacingStore::new("token-b"));
    let service = OwnerRevocationFixture::service(store);
    OwnerRevocationFixture::add_three_tokens(&service).await;

    let revoked = OwnerRevocationFixture::revoke_owner(&service)
        .await
        .unwrap();

    assert_eq!(revoked.len(), 2);
    for id in ["token-a", "token-b", "token-c"] {
        assert!(OwnerRevocationFixture::assert_revoked(&service, id).await);
    }
}
