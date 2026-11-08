use super::support_tests::TestFixture;
use crate::{
    MemoryTokenAuditLog, TokenAuditAction, TokenAuditEvent, TokenExpiry, TokenOptions, TokenScope,
    api_token::MAX_TOKEN_ID_LENGTH, audit::MAX_AUDIT_DETAIL_LENGTH,
};
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
        .map(|event| event.token_id().unwrap().to_string())
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

#[test]
fn audit_events_bound_untrusted_fields() {
    let event = TokenAuditEvent::new(TokenAuditAction::AuthenticationFailed, Utc::now())
        .with_token("bad/id", Some("bad\nowner"))
        .failure("x".repeat(MAX_AUDIT_DETAIL_LENGTH + 10));

    assert_eq!(event.token_id(), None);
    assert_eq!(event.owner(), None);
    assert_eq!(
        event.detail().unwrap().chars().count(),
        MAX_AUDIT_DETAIL_LENGTH
    );
}

#[tokio::test]
async fn invalid_token_ids_are_not_copied_into_audit_events() {
    let audit = Arc::new(MemoryTokenAuditLog::new(4096));
    let service = TestFixture::service_with_audit(audit.clone());
    let token_id = "a".repeat(MAX_TOKEN_ID_LENGTH + 1);

    let error = service
        .authenticate(&token_id, "secret", None, Utc::now())
        .await;

    assert!(error.is_err());
    let event = audit.events().pop().expect("an audit event");
    assert_eq!(event.action, TokenAuditAction::AuthenticationFailed);
    assert_eq!(event.token_id(), None);
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
    let options = TokenOptions::new(
        TokenExpiry::At(now - Duration::seconds(1)),
        TokenScope::DEFAULT,
    );
    TestFixture::add_token_at(&service, "expired", "owner-a", now, options).await;
    audit.drain();

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 1);
    let events = audit.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, TokenAuditAction::Deleted);
    assert_eq!(events[0].token_id(), Some("expired"));
    assert_eq!(events[0].owner(), Some("owner-a"));
}
