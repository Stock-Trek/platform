use super::support_tests::TestFixture;
use crate::{
    AuthService, MemoryTokenAuditLog, RevocationReason, RevokedBy, TokenAuditAction, TokenExpiry,
    TokenOptions, TokenScope,
};
use chrono::{Duration, Utc};
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
    let options = TokenOptions::new(
        TokenExpiry::At(now - Duration::seconds(1)),
        TokenScope::DEFAULT,
    );
    let expired = TestFixture::add_token_at(&service, "token-b", "owner-a", now, options).await;

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
    let revoked = service
        .revoke_token(
            token.id(),
            RevocationReason::NoLongerNeeded,
            RevokedBy::Owner,
            second,
        )
        .await
        .unwrap();
    let stored = service.token(token.id()).await.unwrap();

    assert_eq!(revoked.metadata().revoked_at(), Some(first));
    assert_eq!(
        revoked.metadata().revocation_reason(),
        Some(RevocationReason::Compromised)
    );
    assert_eq!(
        revoked.metadata().revoked_by(),
        Some(RevokedBy::SecurityTeam)
    );
    assert_eq!(stored.metadata().revoked_at(), Some(first));
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
    service
        .revoke_token(
            token.id(),
            RevocationReason::NoLongerNeeded,
            RevokedBy::Owner,
            now,
        )
        .await
        .unwrap();
    let events: Vec<_> = audit
        .events()
        .into_iter()
        .filter(|event| {
            event.action == TokenAuditAction::Revoked && event.token_id() == Some(token.id())
        })
        .collect();

    assert_eq!(events.len(), 1);
    assert!(events[0].success);
}
