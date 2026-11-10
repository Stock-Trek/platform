use super::support_tests::TestFixture;
use crate::{MemoryTokenAuditLog, TokenAuditAction};
use chrono::{Duration, Utc};
use std::sync::Arc;

#[tokio::test]
async fn keeps_only_the_most_recent_events_at_capacity() {
    let audit = Arc::new(MemoryTokenAuditLog::new(2));
    let service = TestFixture::service();

    TestFixture::add_token(&service, "token-a", "owner-a").await;
    TestFixture::drain(&service, audit.clone()).await;
    TestFixture::add_token(&service, "token-b", "owner-a").await;
    TestFixture::drain(&service, audit.clone()).await;
    TestFixture::add_token(&service, "token-c", "owner-a").await;
    TestFixture::drain(&service, audit.clone()).await;

    let ids: Vec<_> = audit
        .events()
        .into_iter()
        .map(|event| event.token_id.unwrap())
        .collect();
    assert_eq!(ids, vec!["token-b", "token-c"]);
}

#[tokio::test]
async fn deleting_a_missing_token_does_not_record_a_deletion() {
    let audit = Arc::new(MemoryTokenAuditLog::new(4096));
    let service = TestFixture::service();

    service.delete_token("ghost", Utc::now()).await.unwrap();
    TestFixture::drain(&service, audit.clone()).await;

    assert!(audit.events().is_empty());
}

#[tokio::test]
async fn purging_expired_tokens_records_deletions() {
    let audit = Arc::new(MemoryTokenAuditLog::new(4096));
    let service = TestFixture::service();
    let now = Utc::now();
    TestFixture::add_expired_token(
        &service,
        "expired",
        "owner-a",
        now - Duration::seconds(2),
        now - Duration::seconds(1),
    )
    .await;

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 1);
    TestFixture::drain(&service, audit.clone()).await;
    let event = audit
        .events()
        .into_iter()
        .find(|event| event.action == TokenAuditAction::Deleted)
        .expect("a deletion event should be recorded");
    assert_eq!(event.token_id.as_deref(), Some("expired"));
    assert_eq!(event.owner.as_deref(), Some("owner-a"));
}
