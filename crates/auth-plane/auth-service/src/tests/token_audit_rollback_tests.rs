use super::support_tests::TestFixture;
use crate::{RevocationReason, RevokedBy, TokenAuditAction, TokenAuditEvent, TokenAuditLog};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError, BpResult};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, Ordering},
};

const NO_FAILURE: u8 = u8::MAX;

struct SelectiveAuditLog {
    fail_on: AtomicU8,
    events: Mutex<Vec<TokenAuditEvent>>,
}

impl SelectiveAuditLog {
    fn new() -> Self {
        Self {
            fail_on: AtomicU8::new(NO_FAILURE),
            events: Mutex::new(Vec::new()),
        }
    }

    fn fail_on(&self, action: TokenAuditAction) {
        self.fail_on.store(action as u8, Ordering::SeqCst);
    }

    fn events(&self) -> Vec<TokenAuditEvent> {
        self.events.lock().expect("audit events lock").clone()
    }

    fn should_fail(&self, event: &TokenAuditEvent) -> bool {
        self.fail_on.load(Ordering::SeqCst) == event.action as u8
    }

    fn error() -> BpError {
        BpError::auth_invalid_field("audit", "queue unavailable")
    }
}

#[async_trait]
impl TokenAuditLog for SelectiveAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        if self.should_fail(&event) {
            return Err(Self::error());
        }
        self.events.lock().expect("audit events lock").push(event);
        Ok(())
    }

    async fn record_batch(&self, events: &[TokenAuditEvent]) -> BpResult<()> {
        if events.iter().any(|event| self.should_fail(event)) {
            return Err(Self::error());
        }
        self.events
            .lock()
            .expect("audit events lock")
            .extend_from_slice(events);
        Ok(())
    }
}

struct AuditRollbackFixture;

impl AuditRollbackFixture {
    fn service() -> (crate::AuthService, Arc<SelectiveAuditLog>) {
        let audit = Arc::new(SelectiveAuditLog::new());
        let service = TestFixture::service_with_audit(audit.clone());
        (service, audit)
    }

    fn assert_audit_error(error: BpError) {
        assert!(matches!(
            error,
            BpError::Auth(AuthError::InvalidField { field: "audit", .. })
        ));
    }
}

#[tokio::test]
async fn add_token_rolls_back_when_audit_issuance_fails() {
    let (service, audit) = AuditRollbackFixture::service();
    let now = Utc::now();
    audit.fail_on(TokenAuditAction::Issued);

    let error = service
        .add_token("token-a", "owner-a", now, TestFixture::options(), now)
        .await
        .unwrap_err();

    AuditRollbackFixture::assert_audit_error(error);
    assert!(matches!(
        service.token("token-a").await,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn delete_token_rolls_back_when_audit_deletion_fails() {
    let (service, audit) = AuditRollbackFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    audit.fail_on(TokenAuditAction::Deleted);

    let error = service
        .delete_token(token.id(), Utc::now())
        .await
        .unwrap_err();

    AuditRollbackFixture::assert_audit_error(error);
    assert!(service.token(token.id()).await.is_ok());
}

#[tokio::test]
async fn revoke_token_rolls_back_when_audit_revocation_fails() {
    let (service, audit) = AuditRollbackFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    audit.fail_on(TokenAuditAction::Revoked);

    let error = service
        .revoke_token(
            token.id(),
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            Utc::now(),
        )
        .await
        .unwrap_err();

    AuditRollbackFixture::assert_audit_error(error);
    assert!(
        !service
            .token(token.id())
            .await
            .unwrap()
            .metadata()
            .is_revoked()
    );
}

#[tokio::test]
async fn purge_expired_tokens_rolls_back_when_audit_deletion_fails() {
    let (service, audit) = AuditRollbackFixture::service();
    let now = Utc::now();
    for id in ["expired-a", "expired-b"] {
        TestFixture::add_expired_token(
            &service,
            id,
            "owner-a",
            now - Duration::seconds(2),
            now - Duration::seconds(1),
        )
        .await;
    }
    audit.fail_on(TokenAuditAction::Deleted);

    let error = service.purge_expired_tokens(now).await.unwrap_err();

    AuditRollbackFixture::assert_audit_error(error);
    assert!(service.token("expired-a").await.is_ok());
    assert!(service.token("expired-b").await.is_ok());
}

#[tokio::test]
async fn rotation_rolls_back_both_tokens_when_audit_fails() {
    let (service, audit) = AuditRollbackFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    audit.fail_on(TokenAuditAction::Issued);

    let error = service
        .rotate_token_to(token.id(), "token-b", Utc::now())
        .await
        .unwrap_err();

    AuditRollbackFixture::assert_audit_error(error);
    assert!(
        service
            .token(token.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
    assert!(matches!(
        service.token("token-b").await,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
    assert!(
        audit
            .events()
            .iter()
            .all(|event| event.action != TokenAuditAction::Rotated)
    );
}
