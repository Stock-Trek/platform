use super::support_tests::{AuthServiceBuilder, ORDER_REQUEST, TestFixture};
use crate::{
    ApiToken, AuthService, MemoryTokenStore, TokenAuditAction, TokenAuditEvent, TokenMutation,
    TokenRotationTransaction, TokenStore,
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct SecretRotationStore {
    inner: MemoryTokenStore,
    race_next_update: AtomicBool,
}

impl SecretRotationStore {
    fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            race_next_update: AtomicBool::new(false),
        }
    }

    fn rotate_secret_next_update(&self) {
        self.race_next_update.store(true, Ordering::SeqCst);
    }

    async fn rotate_secret(&self, expected: &ApiToken) -> BpResult<()> {
        let Some(mut current) = self.inner.get(expected.id()).await? else {
            return Err(BpError::unknown_token(expected.id()));
        };
        current.secret = SecretString::from("rotated-secret");
        self.inner.update(expected, &current).await
    }
}

#[async_trait]
impl TokenStore for SecretRotationStore {
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
            self.rotate_secret(expected).await?;
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

struct SignatureCommitRaceFixture;

impl SignatureCommitRaceFixture {
    fn service() -> (AuthService, Arc<SecretRotationStore>) {
        let store = Arc::new(SecretRotationStore::new());
        let service = AuthServiceBuilder::new().store(store.clone()).build();
        (service, store)
    }
}

#[tokio::test]
async fn rejects_a_signature_when_the_secret_changes_during_the_commit() {
    let (service, store) = SignatureCommitRaceFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    store.rotate_secret_next_update();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("verification completes");

    assert!(!verified);
    let stored = service.token(token.id()).await.expect("stored token");
    assert!(!stored.matches_secret(token.secret().expose_secret()));
}

#[tokio::test]
async fn a_rejected_signature_commit_releases_the_replay_reservation() {
    let (service, store) = SignatureCommitRaceFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    store.rotate_secret_next_update();

    assert!(
        !service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("verification completes")
    );

    let stored = service.token(token.id()).await.expect("stored token");
    let resigned = service
        .sign_order_request(token.id(), stored.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed with the current secret");
    assert!(
        service
            .verify_order_request(token.id(), ORDER_REQUEST, resigned.envelope(), now)
            .await
            .expect("verification completes")
    );
}

#[tokio::test]
async fn a_rejected_signature_commit_is_not_recorded_as_verified() {
    let audit = Arc::new(crate::MemoryTokenAuditLog::new(4096));
    let store = Arc::new(SecretRotationStore::new());
    let service = AuthServiceBuilder::new().store(store.clone()).build();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    store.rotate_secret_next_update();

    assert!(
        !service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("verification completes")
    );
    TestFixture::drain(&service, audit.clone()).await;

    let actions: Vec<TokenAuditAction> = audit
        .events()
        .into_iter()
        .map(|event| event.action)
        .collect();
    assert!(!actions.contains(&TokenAuditAction::SignatureVerified));
    assert!(actions.contains(&TokenAuditAction::SignatureRejected));
}
