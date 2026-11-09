use super::support_tests::TestFixture;
use crate::{MemoryTokenAuditLog, TokenAuditAction, TokenAuditEvent, TokenAuditLog};
use chrono::{Duration, Utc};
use std::sync::Arc;

#[tokio::test]
async fn keeps_only_the_most_recent_events_at_capacity() {
    let audit = Arc::new(MemoryTokenAuditLog::new(2));
    let service = TestFixture::service_with_audit(audit.clone());

    TestFixture::add_token(&service, "token-a", "owner-a").await;
    TestFixture::add_token(&service, "token-b", "owner-a").await;
    TestFixture::add_token(&service, "token-c", "owner-a").await;

    let ids: Vec<_> = audit
        .drain()
        .into_iter()
        .map(|event| event.token_id.unwrap())
        .collect();
    assert_eq!(ids, vec!["token-b", "token-c"]);
}

#[tokio::test]
async fn audit_events_can_be_drained() {
    let audit = Arc::new(MemoryTokenAuditLog::new(4096));
    let service = TestFixture::service_with_audit(audit.clone());
    TestFixture::add_token(&service, "token-a", "owner-a").await;

    assert_eq!(audit.drain().len(), 1);
    assert!(audit.events().is_empty());
}

#[tokio::test]
async fn deleting_a_missing_token_does_not_record_a_deletion() {
    let audit = Arc::new(MemoryTokenAuditLog::new(4096));
    let service = TestFixture::service_with_audit(audit.clone());

    service.delete_token("ghost", Utc::now()).await.unwrap();

    assert!(audit.events().is_empty());
}

#[tokio::test]
async fn purging_expired_tokens_records_deletions() {
    let audit = Arc::new(MemoryTokenAuditLog::new(4096));
    let service = TestFixture::service_with_audit(audit.clone());
    let now = Utc::now();
    TestFixture::add_expired_token(
        &service,
        "expired",
        "owner-a",
        now - Duration::seconds(2),
        now - Duration::seconds(1),
    )
    .await;
    audit.drain();

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 1);
    let events = audit.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, TokenAuditAction::Deleted);
    assert_eq!(events[0].token_id.as_deref(), Some("expired"));
    assert_eq!(events[0].owner.as_deref(), Some("owner-a"));
}

#[tokio::test]
async fn audit_batches_are_recorded_together() {
    let audit = MemoryTokenAuditLog::new(1);
    let now = Utc::now();
    let events = [
        TokenAuditEvent::new(TokenAuditAction::Rotated, now),
        TokenAuditEvent::new(TokenAuditAction::Issued, now),
    ];

    audit.record_batch(&events).await.unwrap();

    let recorded = audit.events();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].action, TokenAuditAction::Issued);
}
