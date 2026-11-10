use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{
    ApiToken, AuthService, ConsumeResult, MemoryReplayStore, MemoryTokenStore, ReplayKey,
    ReplayReservation, ReplayStore, ReplayStoreError, TokenAuditAction, TokenAuditEvent,
    TokenAuditLog, TokenStore,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::{AuthError, BpError, BpResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct FailingReleaseReplayStore {
    inner: MemoryReplayStore,
    release_attempts: AtomicUsize,
}

impl FailingReleaseReplayStore {
    fn new() -> Self {
        Self {
            inner: MemoryReplayStore::default(),
            release_attempts: AtomicUsize::new(0),
        }
    }

    fn release_attempts(&self) -> usize {
        self.release_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ReplayStore for FailingReleaseReplayStore {
    async fn consume(
        &self,
        key: ReplayKey,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<ConsumeResult, ReplayStoreError> {
        self.inner.consume(key, expires_at, now).await
    }

    async fn release(&self, _reservation: &ReplayReservation) -> Result<(), ReplayStoreError> {
        self.release_attempts.fetch_add(1, Ordering::SeqCst);
        Err(ReplayStoreError::unavailable(
            "release",
            true,
            std::io::Error::other("release failed"),
        ))
    }
}

struct FailingSignatureVerifiedAuditLog;

#[async_trait]
impl TokenAuditLog for FailingSignatureVerifiedAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        if event.action == TokenAuditAction::SignatureVerified {
            return Err(BpError::auth_invalid_field("audit", "queue unavailable"));
        }
        Ok(())
    }
}

struct ConflictReadStore {
    inner: MemoryTokenStore,
    conflict_next_update: AtomicBool,
    fail_read_after_update: AtomicBool,
}

impl ConflictReadStore {
    fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            conflict_next_update: AtomicBool::new(false),
            fail_read_after_update: AtomicBool::new(false),
        }
    }

    fn conflict_next_update(&self) {
        self.conflict_next_update.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl TokenStore for ConflictReadStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        if self.fail_read_after_update.swap(false, Ordering::SeqCst) {
            return Err(BpError::auth_store("read", id, true, "read failed"));
        }
        self.inner.get(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        self.inner.delete(id).await
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        if self.conflict_next_update.swap(false, Ordering::SeqCst) {
            self.fail_read_after_update.store(true, Ordering::SeqCst);
            return Err(BpError::token_conflict(token.id()));
        }
        self.inner.update(expected, token).await
    }
}

#[tokio::test]
async fn audit_failure_takes_precedence_over_replay_release_failure() {
    let replay = Arc::new(FailingReleaseReplayStore::new());
    let service = AuthService::new(
        Arc::new(MemoryTokenStore::default()),
        TestFixture::config_with_audit(Arc::new(FailingSignatureVerifiedAuditLog)),
        replay.clone(),
    )
    .expect("valid service");
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let error = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect_err("audit delivery failure");

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField { field: "audit", .. })
    ));
    assert_eq!(replay.release_attempts(), 1);
}

#[tokio::test]
async fn read_failure_takes_precedence_over_replay_release_failure() {
    let replay = Arc::new(FailingReleaseReplayStore::new());
    let store = Arc::new(ConflictReadStore::new());
    let service = AuthService::new(store.clone(), TestFixture::config(), replay.clone())
        .expect("valid service");
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    store.conflict_next_update();

    let error = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect_err("token read failure");

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Store {
            operation: "read",
            ..
        })
    ));
    assert_eq!(replay.release_attempts(), 1);
}
