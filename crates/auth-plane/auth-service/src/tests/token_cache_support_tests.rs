use crate::{ApiToken, CachedTokenStore, MemoryTokenStore, TokenCacheConfig, TokenStore};
use async_trait::async_trait;
use platform_errors::{AuthError, BpError, BpResult};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

pub(crate) struct TestStore {
    inner: MemoryTokenStore,
    get_calls: AtomicUsize,
    fail: AtomicBool,
    permanent_fail: AtomicBool,
    delay_millis: AtomicU64,
}

impl TestStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            get_calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            permanent_fail: AtomicBool::new(false),
            delay_millis: AtomicU64::new(0),
        }
    }

    pub(crate) async fn insert(&self, token: &ApiToken) {
        self.inner.put(token).await.unwrap();
    }

    pub(crate) fn get_calls(&self) -> usize {
        self.get_calls.load(Ordering::SeqCst)
    }

    pub(crate) fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }

    pub(crate) fn set_permanent_fail(&self, fail: bool) {
        self.permanent_fail.store(fail, Ordering::SeqCst);
    }

    pub(crate) fn set_delay_millis(&self, delay: u64) {
        self.delay_millis.store(delay, Ordering::SeqCst);
    }
}

#[async_trait]
impl TokenStore for TestStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        let delay = self.delay_millis.load(Ordering::SeqCst);
        if self.permanent_fail.load(Ordering::SeqCst) {
            return Err(BpError::auth_store(
                "read",
                id,
                false,
                std::io::Error::other("permanent read failure"),
            ));
        }
        if self.fail.load(Ordering::SeqCst) {
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            return Err(BpError::Auth(AuthError::Unavailable {
                retry_after: Duration::from_secs(1),
            }));
        }
        let result = self.inner.get(id).await;
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        result
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        self.inner.delete(id).await
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        self.inner.update(expected, token).await
    }

    async fn commit(
        &self,
        mutation: crate::TokenMutation,
        event: crate::TokenAuditEvent,
    ) -> BpResult<bool> {
        self.inner.commit(mutation, event).await
    }

    async fn commit_rotation(&self, transaction: &crate::TokenRotationTransaction) -> BpResult<()> {
        self.inner.commit_rotation(transaction).await
    }
}

pub(crate) struct Fixture;

impl Fixture {
    pub(crate) fn config(
        freshness_millis: u64,
        max_stale_millis: u64,
        failure_threshold: usize,
        open_millis: u64,
    ) -> TokenCacheConfig {
        TokenCacheConfig::new(
            16,
            16,
            Duration::from_millis(freshness_millis),
            Duration::from_millis(max_stale_millis),
            failure_threshold,
            Duration::from_millis(open_millis),
        )
        .unwrap()
    }

    pub(crate) fn cached(store: Arc<TestStore>, config: TokenCacheConfig) -> CachedTokenStore {
        CachedTokenStore::new(store, config)
    }
}
