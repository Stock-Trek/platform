use crate::{ApiToken, CachedTokenStore, MemoryTokenStore, TokenCacheConfig, TokenStore};
use async_trait::async_trait;
use platform_errors::{AuthError, BuildError, BuildResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Notify, watch};

pub(crate) struct TestStore {
    inner: MemoryTokenStore,
    get_calls: AtomicUsize,
    fail: AtomicBool,
    permanent_fail: AtomicBool,
    hold: AtomicBool,
    started: Notify,
    released: watch::Sender<bool>,
}

impl TestStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            get_calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            permanent_fail: AtomicBool::new(false),
            hold: AtomicBool::new(false),
            started: Notify::new(),
            released: watch::channel(false).0,
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

    pub(crate) fn hold_reads(&self) {
        self.hold.store(true, Ordering::SeqCst);
    }

    pub(crate) fn release_reads(&self) {
        self.released.send_replace(true);
    }

    pub(crate) async fn wait_until_read_starts(&self) {
        self.started.notified().await;
    }

    async fn hold_read_if_requested(&self) {
        if !self.hold.load(Ordering::SeqCst) {
            return;
        }
        self.started.notify_one();
        let mut released = self.released.subscribe();
        loop {
            if *released.borrow() {
                return;
            }
            if released.changed().await.is_err() {
                return;
            }
        }
    }
}

#[async_trait]
impl TokenStore for TestStore {
    async fn put(&self, token: &ApiToken) -> BuildResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BuildResult<Option<ApiToken>> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        if self.permanent_fail.load(Ordering::SeqCst) {
            return Err(BuildError::auth_store(
                "read",
                id,
                false,
                std::io::Error::other("permanent read failure"),
            ));
        }
        if self.fail.load(Ordering::SeqCst) {
            self.hold_read_if_requested().await;
            return Err(BuildError::Auth(AuthError::Unavailable {
                retry_after: Duration::from_secs(1),
            }));
        }
        let result = self.inner.get(id).await;
        self.hold_read_if_requested().await;
        result
    }

    async fn delete(&self, id: &str) -> BuildResult<()> {
        self.inner.delete(id).await
    }

    async fn ids(&self) -> BuildResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BuildResult<()> {
        self.inner.update(expected, token).await
    }

    async fn commit(
        &self,
        mutation: crate::TokenMutation,
        event: crate::TokenAuditEvent,
    ) -> BuildResult<bool> {
        self.inner.commit(mutation, event).await
    }

    async fn commit_rotation(
        &self,
        transaction: &crate::TokenRotationTransaction,
    ) -> BuildResult<()> {
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

    pub(crate) async fn wait_for_started(started: &AtomicUsize, count: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("tasks should start");
    }
}
