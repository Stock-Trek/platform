use super::support_tests::TestFixture;
use crate::{AuthService, MemoryTokenAuditLog, RevocationReason, RevokedBy, TokenAuditAction};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};
use std::sync::Arc;

struct RevocationFixture;

impl RevocationFixture {
    fn service() -> (AuthService, Arc<MemoryTokenAuditLog>) {
        let audit = Arc::new(MemoryTokenAuditLog::new(64));
        let service = TestFixture::service_with_audit(audit.clone());
        (service, audit)
    }
}

#[tokio::test]
async fn revoking_an_owners_tokens_includes_expired_tokens() {
    let (service, _) = RevocationFixture::service();
    let now = Utc::now();
    let active = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let expired = TestFixture::add_expired_token(
        &service,
        "token-b",
        "owner-a",
        now - Duration::seconds(2),
        now - Duration::seconds(1),
    )
    .await;

    let revoked = service
        .revoke_tokens_for_owner(
            "owner-a",
            RevocationReason::NoLongerNeeded,
            RevokedBy::Owner,
            now,
        )
        .await
        .unwrap();

    assert_eq!(revoked.len(), 2);
    assert!(
        service
            .token(active.id())
            .await
            .unwrap()
            .metadata()
            .is_revoked()
    );
    assert!(
        service
            .token(expired.id())
            .await
            .unwrap()
            .metadata()
            .is_revoked()
    );
}

#[tokio::test]
async fn revoking_a_token_twice_preserves_the_first_revocation() {
    let (service, _) = RevocationFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let first = Utc::now();
    let second = first + Duration::minutes(1);

    service
        .revoke_token(
            token.id(),
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            first,
        )
        .await
        .unwrap();
    let error = service
        .revoke_token(
            token.id(),
            RevocationReason::NoLongerNeeded,
            RevokedBy::Owner,
            second,
        )
        .await;
    let stored = service.token(token.id()).await.unwrap();

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        }))
    ));
    assert_eq!(stored.metadata().revoked_at(), Some(first));
    assert_eq!(
        stored.metadata().revocation_reason(),
        Some(RevocationReason::Compromised)
    );
    assert_eq!(
        stored.metadata().revoked_by(),
        Some(RevokedBy::SecurityTeam)
    );
}

#[tokio::test]
async fn revoking_a_token_twice_records_one_revocation_event() {
    let (service, audit) = RevocationFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    service
        .revoke_token(
            token.id(),
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();
    let error = service
        .revoke_token(
            token.id(),
            RevocationReason::NoLongerNeeded,
            RevokedBy::Owner,
            now,
        )
        .await;

    assert!(error.is_err());
    let events: Vec<_> = audit
        .events()
        .into_iter()
        .filter(|event| {
            event.action == TokenAuditAction::Revoked
                && event.token_id.as_deref() == Some(token.id())
        })
        .collect();

    assert_eq!(events.len(), 1);
    assert!(events[0].success);
}
