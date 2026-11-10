use super::support_tests::{AuthServiceBuilder, ORDER_REQUEST, TestFixture};
use crate::{
    ApiToken, AuthService, MemoryTokenStore, RevocationReason, RevokedBy, TokenAuditEvent,
    TokenMutation, TokenStore,
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{AuthError, BpError, BpResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Copy)]
enum RaceMode {
    Revoke,
    Touch,
}

struct RaceStore {
    inner: MemoryTokenStore,
    race_next_update: AtomicBool,
    mode: RaceMode,
}

impl RaceStore {
    fn new(mode: RaceMode) -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            race_next_update: AtomicBool::new(false),
            mode,
        }
    }

    fn race_next_update(&self) {
        self.race_next_update.store(true, Ordering::SeqCst);
    }

    async fn apply_race(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        let Some(mut current) = self.inner.get(token.id()).await? else {
            return Err(BpError::unknown_token(token.id()));
        };
        match self.mode {
            RaceMode::Revoke => current.metadata.revoke(
                Utc::now(),
                RevocationReason::Compromised,
                RevokedBy::SecurityTeam,
            ),
            RaceMode::Touch => current.metadata.record_use(Utc::now()),
        }
        self.inner.update(expected, &current).await
    }
}

#[async_trait]
impl TokenStore for RaceStore {
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
        if self.race_next_update.swap(false, Ordering::SeqCst) {
            self.apply_race(expected, token).await?;
        }
        self.inner.update(expected, token).await
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

struct CasFixture;

impl CasFixture {
    fn service(mode: RaceMode) -> (AuthService, Arc<RaceStore>) {
        let store = Arc::new(RaceStore::new(mode));
        let service = AuthServiceBuilder::new().store(store.clone()).build();
        (service, store)
    }

    fn assert_conflict<T>(result: BpResult<T>) {
        assert!(matches!(
            result,
            Err(BpError::Auth(AuthError::Conflict { .. }))
        ));
    }
}

#[tokio::test]
async fn memory_store_rejects_a_stale_update() {
    let store = MemoryTokenStore::default();
    let token = TestFixture::token("token-a");
    store.put(&token).await.unwrap();
    let now = Utc::now();
    let mut first = token.clone();
    first.metadata.record_use(now);
    store.update(&token, &first).await.unwrap();
    let mut stale = token.clone();
    stale
        .metadata
        .record_use(now + chrono::Duration::seconds(1));

    CasFixture::assert_conflict(store.update(&token, &stale).await);

    let stored = store.get(token.id()).await.unwrap().unwrap();
    assert_eq!(stored.metadata().last_used_at(), Some(now));
}

#[tokio::test]
async fn authentication_does_not_overwrite_a_concurrent_revocation() {
    let (service, store) = CasFixture::service(RaceMode::Revoke);
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    store.race_next_update();

    let error = service
        .authenticate(token.id(), token.secret(), None, Utc::now())
        .await
        .unwrap_err();

    assert!(matches!(error, BpError::Auth(AuthError::Revoked { .. })));
    assert!(
        service
            .token(token.id())
            .await
            .unwrap()
            .metadata()
            .is_revoked()
    );
}

#[tokio::test]
async fn revocation_retries_after_a_concurrent_last_used_update() {
    let (service, store) = CasFixture::service(RaceMode::Touch);
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    store.race_next_update();
    let now = Utc::now();

    let revoked = service
        .revoke_token(
            token.id(),
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();

    let stored = service.token(token.id()).await.unwrap();
    assert!(revoked.metadata().is_revoked());
    assert!(stored.metadata().is_revoked());
    assert_eq!(stored.metadata().revoked_at(), Some(now));
    assert!(stored.metadata().last_used_at().is_some());
}

#[tokio::test]
async fn signature_verification_retries_after_a_concurrent_last_used_update() {
    let (service, store) = CasFixture::service(RaceMode::Touch);
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();
    store.race_next_update();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();

    assert!(verified);
    assert!(
        service
            .token(token.id())
            .await
            .unwrap()
            .metadata()
            .last_used_at()
            .is_some()
    );
}

#[tokio::test]
async fn rotation_retries_after_a_concurrent_last_used_update() {
    let (service, store) = CasFixture::service(RaceMode::Touch);
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    store.race_next_update();
    let now = Utc::now();

    let replacement = service
        .rotate_token_to(token.id(), "token-b", now)
        .await
        .unwrap();

    assert_eq!(replacement.metadata().rotated_from(), Some(token.id()));
    assert_eq!(
        service
            .token(token.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by(),
        Some(replacement.id())
    );
}
