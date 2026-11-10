use super::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{SecretInput, TokenAuditAction, TokenAuditEvent, TokenAuditLog};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::BpResult;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

struct ControllableAuditLog {
    failing: AtomicBool,
    events: Mutex<Vec<TokenAuditEvent>>,
}

impl ControllableAuditLog {
    fn new() -> Self {
        Self {
            failing: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
        }
    }

    fn fail(&self) {
        self.failing.store(true, Ordering::SeqCst);
    }

    fn succeed(&self) {
        self.failing.store(false, Ordering::SeqCst);
    }

    fn events(&self) -> Vec<TokenAuditEvent> {
        self.events.lock().expect("audit lock").clone()
    }
}

#[async_trait]
impl TokenAuditLog for ControllableAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(platform_errors::BpError::auth_store(
                "audit",
                "outbox-test",
                true,
                "sink unavailable".to_string(),
            ));
        }
        self.events.lock().expect("audit lock").push(event);
        Ok(())
    }
}

struct OutboxFixture;

impl OutboxFixture {
    fn service() -> (crate::AuthService, Arc<ControllableAuditLog>) {
        let audit = Arc::new(ControllableAuditLog::new());
        let service = AuthServiceBuilder::new().audit(audit.clone()).build();
        (service, audit)
    }
}

#[tokio::test]
async fn a_failed_delivery_leaves_the_event_in_the_outbox() {
    let (service, audit) = OutboxFixture::service();
    audit.fail();
    let now = Utc::now();

    let token = service
        .add_token("token-a", "owner-a", now, TestFixture::options(), now)
        .await
        .unwrap();

    assert!(audit.events().is_empty());
    let pending = service.store.pending_audits().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].action, TokenAuditAction::Issued);
    assert_eq!(pending[0].token_id.as_deref(), Some(token.id()));
    let event_id = pending[0].event_id().expect("event id").to_string();

    audit.succeed();
    assert_eq!(service.drain_audit_outbox().await.unwrap(), 1);
    let delivered = audit.events();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].event_id(), Some(event_id.as_str()));
    assert!(service.store.pending_audits().await.unwrap().is_empty());
    assert_eq!(service.drain_audit_outbox().await.unwrap(), 0);
    assert_eq!(audit.events().len(), 1);
}

#[tokio::test]
async fn authentication_appends_to_the_same_outbox() {
    let (service, audit) = OutboxFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    audit.fail();

    service
        .authenticate(token.id(), token.secret().secret_input(), None, Utc::now())
        .await
        .unwrap();

    let pending = service.store.pending_audits().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].action, TokenAuditAction::Authenticated);

    audit.succeed();
    assert_eq!(service.drain_audit_outbox().await.unwrap(), 1);
    assert_eq!(
        audit.events().last().unwrap().action,
        TokenAuditAction::Authenticated
    );
}

#[tokio::test]
async fn a_deleted_token_keeps_its_tombstone_until_the_event_is_delivered() {
    let (service, audit) = OutboxFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    audit.fail();

    service.delete_token(token.id(), Utc::now()).await.unwrap();

    assert!(service.store.get(token.id()).await.unwrap().is_none());
    let pending = service.store.pending_audits().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].action, TokenAuditAction::Deleted);
    assert!(service.store.put(&token).await.is_err());

    audit.succeed();
    assert_eq!(service.drain_audit_outbox().await.unwrap(), 1);
    assert_eq!(
        audit.events().last().unwrap().action,
        TokenAuditAction::Deleted
    );
    service.store.put(&token).await.unwrap();
    assert_eq!(
        service
            .store
            .get(token.id())
            .await
            .unwrap()
            .unwrap()
            .to_value(),
        token.to_value()
    );
}

#[tokio::test]
async fn purge_commits_tokens_one_at_a_time_without_restoring_them() {
    let (service, audit) = OutboxFixture::service();
    let now = Utc::now();
    for id in ["expired-a", "expired-b"] {
        TestFixture::add_expired_token(
            &service,
            id,
            "owner-a",
            now - chrono::Duration::seconds(2),
            now - chrono::Duration::seconds(1),
        )
        .await;
    }
    audit.fail();

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 2);
    for id in ["expired-a", "expired-b"] {
        assert!(service.store.get(id).await.unwrap().is_none());
    }
    assert_eq!(service.store.pending_audits().await.unwrap().len(), 2);

    audit.succeed();
    assert_eq!(service.drain_audit_outbox().await.unwrap(), 2);
    assert_eq!(audit.events().len(), 2);
    assert!(
        audit
            .events()
            .iter()
            .all(|event| event.action == TokenAuditAction::Deleted)
    );
}
