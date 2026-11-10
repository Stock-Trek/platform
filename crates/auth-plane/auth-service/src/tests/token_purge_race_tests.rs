use super::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{
    ApiToken, AuthService, MemoryTokenStore, TokenAuditEvent, TokenMetadata, TokenMutation,
    TokenRotationTransaction, TokenScope, TokenStore,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use platform_errors::BpResult;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct RecreateOnSecondGet {
    inner: MemoryTokenStore,
    replacement: ApiToken,
    gets: AtomicUsize,
}

impl RecreateOnSecondGet {
    fn new(replacement: ApiToken) -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            replacement,
            gets: AtomicUsize::new(0),
        }
    }

    async fn recreate_if_due(&self, id: &str) -> BpResult<()> {
        if self.gets.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
            self.inner.delete(id).await?;
            self.inner.put(&self.replacement).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl TokenStore for RecreateOnSecondGet {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.recreate_if_due(id).await?;
        self.inner.get(id).await
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

struct PurgeRaceFixture;

impl PurgeRaceFixture {
    fn service(replacement: ApiToken) -> (AuthService, Arc<RecreateOnSecondGet>) {
        let store = Arc::new(RecreateOnSecondGet::new(replacement));
        let service = AuthServiceBuilder::new().store(store.clone()).build();
        (service, store)
    }

    fn expired_token(now: chrono::DateTime<Utc>) -> ApiToken {
        let metadata = TokenMetadata::new(now - Duration::seconds(1), TokenScope::DEFAULT)
            .expect("valid metadata");
        ApiToken::generate(
            "token-a",
            "expired-owner",
            now - Duration::seconds(2),
            metadata,
        )
        .expect("valid token")
    }
}

#[tokio::test]
async fn purging_skips_a_token_recreated_after_the_scan() {
    let now = Utc::now();
    let expired = PurgeRaceFixture::expired_token(now);
    let replacement = TestFixture::token("token-a");
    let (service, _) = PurgeRaceFixture::service(replacement.clone());
    service.store.put(&expired).await.unwrap();

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 0);
    assert_eq!(
        service.token("token-a").await.unwrap().to_value(),
        replacement.to_value()
    );
}

#[tokio::test]
async fn memory_store_conditional_delete_rejects_a_changed_token() {
    let store = MemoryTokenStore::default();
    let token = TestFixture::token("token-a");
    store.put(&token).await.unwrap();
    let mut changed = token.clone();
    changed
        .metadata
        .record_use(Utc::now() + Duration::seconds(1));
    store.update(&token, &changed).await.unwrap();

    assert!(!store.delete_if(&token).await.unwrap());
    assert!(store.get(token.id()).await.unwrap().is_some());
    assert!(store.delete_if(&changed).await.unwrap());
    assert!(store.get(token.id()).await.unwrap().is_none());
}
