use super::support_tests::TestFixture;
use crate::{
    ApiToken, MemoryTokenAuditLog, MemoryTokenStore, TokenAuditAction, TokenExpiry, TokenOptions,
    TokenScope, TokenStore,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use platform_errors::{BpError, BpResult};
use std::{io, sync::Arc};

struct DeleteFailingStore {
    inner: MemoryTokenStore,
}

#[async_trait]
impl TokenStore for DeleteFailingStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.inner.get(id).await
    }

    async fn delete(&self, _id: &str) -> BpResult<()> {
        Err(BpError::auth_store(
            "delete",
            "tokens",
            true,
            io::Error::other("simulated delete failure"),
        ))
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.update(token).await
    }
}

struct DeleteAuditFixture;

impl DeleteAuditFixture {
    fn service(audit: Arc<MemoryTokenAuditLog>) -> crate::AuthService {
        let store = DeleteFailingStore {
            inner: MemoryTokenStore::default(),
        };
        crate::AuthService::new(Arc::new(store), TestFixture::config_with_audit(audit))
            .expect("valid service")
    }

    fn assert_failure(event: &crate::TokenAuditEvent) {
        assert_eq!(event.action, TokenAuditAction::Deleted);
        assert!(!event.success);
        assert_eq!(event.token_id(), Some("token-a"));
        assert_eq!(event.owner(), Some("demo-owner"));
    }
}

#[tokio::test]
async fn records_a_failed_token_deletion() {
    let audit = Arc::new(MemoryTokenAuditLog::new(16));
    let service = DeleteAuditFixture::service(audit.clone());
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    audit.drain();

    let error = service.delete_token(token.id(), Utc::now()).await;

    assert!(error.is_err());
    let event = audit.events().pop().expect("an audit event");
    DeleteAuditFixture::assert_failure(&event);
}

#[tokio::test]
async fn records_a_failed_expired_token_purge() {
    let audit = Arc::new(MemoryTokenAuditLog::new(16));
    let service = DeleteAuditFixture::service(audit.clone());
    let now = Utc::now();
    let options = TokenOptions::new(
        TokenExpiry::At(now - Duration::seconds(1)),
        TokenScope::DEFAULT,
    );
    TestFixture::add_token_at(&service, "token-a", "demo-owner", now, options).await;
    audit.drain();

    let error = service.purge_expired_tokens(now).await;

    assert!(error.is_err());
    let event = audit.events().pop().expect("an audit event");
    DeleteAuditFixture::assert_failure(&event);
}
