use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{ApiToken, AuthService, MemoryTokenStore, TokenStore};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use std::sync::{
    Arc,
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
}

struct SignatureRetryFixture;

impl SignatureRetryFixture {
    fn service() -> (AuthService, Arc<FailingStore>) {
        let store = Arc::new(FailingStore::new());
        let service =
            AuthService::new(store.clone(), TestFixture::config()).expect("valid service");
        (service, store)
    }
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
