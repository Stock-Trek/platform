use super::{
    sqs_audit_mock_tests::{MockAuditQueue, RecreateOnSecondGet},
    support_tests::{AuthServiceBuilder, TestFixture},
};
use crate::{
    ApiToken, AuthService, SqsTokenAuditLog, TokenAuditAction, TokenAuditEvent, TokenAuditLog,
    TokenMetadata, TokenScope,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::BpError;
use std::{sync::Arc, time::Duration as StdDuration};

struct SqsAuditFixture;

impl SqsAuditFixture {
    fn audit(queue: &Arc<MockAuditQueue>, timeout: StdDuration) -> SqsTokenAuditLog {
        SqsTokenAuditLog::with_queue(queue.clone(), timeout)
    }

    fn service(queue: &Arc<MockAuditQueue>) -> AuthService {
        AuthServiceBuilder::new()
            .audit(Arc::new(Self::audit(queue, StdDuration::from_secs(1))))
            .build()
    }

    fn event(token_id: &str) -> TokenAuditEvent {
        TokenAuditEvent::new(TokenAuditAction::Signed, Utc::now())
            .with_token(token_id, Some("owner-a"))
    }

    fn expired_token(now: DateTime<Utc>) -> ApiToken {
        let metadata = TokenMetadata::new(now - Duration::seconds(1), TokenScope::DEFAULT)
            .expect("valid metadata");
        ApiToken::generate(
            "token-a",
            "expired-owner",
            now - Duration::seconds(2),
            metadata,
        )
        .expect("valid token")
    }
}

#[tokio::test]
async fn record_delivers_one_event_to_the_queue() {
    let queue = Arc::new(MockAuditQueue::new());
    let audit = SqsAuditFixture::audit(&queue, StdDuration::from_secs(1));
    let event = SqsAuditFixture::event("token-a");

    audit.record(event.clone()).await.unwrap();

    assert_eq!(queue.batches(), vec![vec![event]]);
}

#[tokio::test]
async fn record_propagates_queue_delivery_failures() {
    let queue = Arc::new(MockAuditQueue::new());
    queue.fail();
    let audit = SqsAuditFixture::audit(&queue, StdDuration::from_secs(1));

    let error = audit
        .record(SqsAuditFixture::event("token-a"))
        .await
        .unwrap_err();

    assert!(matches!(error, BpError::Sqs(_)));
}

#[tokio::test]
async fn purging_expired_tokens_delivers_one_deletion_event() {
    let queue = Arc::new(MockAuditQueue::new());
    let service = SqsAuditFixture::service(&queue);
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
    assert_eq!(queue.batches().len(), 1);
    let events = queue.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, TokenAuditAction::Deleted);
    assert_eq!(events[0].token_id.as_deref(), Some("expired"));
    assert_eq!(events[0].owner.as_deref(), Some("owner-a"));
}

#[tokio::test]
async fn purging_skips_queue_delivery_for_a_token_recreated_after_the_scan() {
    let now = Utc::now();
    let expired = SqsAuditFixture::expired_token(now);
    let replacement = TestFixture::token("token-a");
    let store = Arc::new(RecreateOnSecondGet::new(replacement.clone()));
    store.put(&expired).await.unwrap();
    let queue = Arc::new(MockAuditQueue::new());
    let service = AuthServiceBuilder::new()
        .store(store)
        .audit(Arc::new(SqsAuditFixture::audit(
            &queue,
            StdDuration::from_secs(1),
        )))
        .build();

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 0);
    assert!(queue.batches().is_empty());
    assert_eq!(
        service.token("token-a").await.unwrap().to_value(),
        replacement.to_value()
    );
}
