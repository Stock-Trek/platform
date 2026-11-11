use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{
    ApiToken, AuthService, ConsumeResult, MemoryReplayStore, MemoryTokenStore, ReplayKey,
    ReplayReservation, ReplayStore, ReplayStoreError, TokenAuditEvent, TokenMutation,
    TokenRotationTransaction, TokenStore,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
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

#[tokio::test]
async fn audit_delivery_failure_does_not_fail_signature_verification() {
    let replay = Arc::new(FailingReleaseReplayStore::new());
    let service = AuthService::new(
        Arc::new(MemoryTokenStore::default()),
        TestFixture::config(),
        replay.clone(),
    )
    .expect("valid service");
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("signature verification succeeds despite audit delivery failure");

    assert!(verified.is_valid());
    assert_eq!(replay.release_attempts(), 0);
}

#[tokio::test]
async fn read_failure_is_reported_before_replay_release_failure() {
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

    let verification = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("token read failure is classified");

    assert!(verification.is_retryable());
    assert_eq!(replay.release_attempts(), 1);
}
