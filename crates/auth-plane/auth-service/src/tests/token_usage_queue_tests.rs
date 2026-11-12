use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{
    MemoryTokenUsageQueue, TokenAuditAction, TokenUsage, TokenUsageQueue, TokenUsageWorker,
};
use chrono::{Duration, Utc};
use std::sync::Arc;

struct UsageQueueFixture;

impl UsageQueueFixture {
    fn service(queue: Arc<MemoryTokenUsageQueue>) -> crate::AuthService {
        TestFixture::service_with_usage(queue)
    }

    async fn worker(
        service: &crate::AuthService,
        queue: Arc<MemoryTokenUsageQueue>,
        batch_size: usize,
    ) -> TokenUsageWorker {
        TokenUsageWorker::new(service.store.clone(), queue, batch_size)
    }
}

#[tokio::test]
async fn authentication_defers_last_used_and_audit_writes_to_the_worker() {
    let queue = Arc::new(MemoryTokenUsageQueue::new(10));
    let service = UsageQueueFixture::service(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    let authenticated = service
        .authenticate(token.id(), token.secret(), None, now)
        .await
        .unwrap();

    assert_eq!(authenticated.metadata().last_used_at(), Some(now));
    assert!(
        service
            .store
            .get(token.id())
            .await
            .unwrap()
            .unwrap()
            .metadata()
            .last_used_at()
            .is_none()
    );
    assert_eq!(queue.len(), 1);

    let worker = UsageQueueFixture::worker(&service, queue.clone(), 100).await;
    assert_eq!(worker.drain().await.unwrap(), 1);

    assert!(queue.is_empty());
    assert_eq!(
        service
            .store
            .get(token.id())
            .await
            .unwrap()
            .unwrap()
            .metadata()
            .last_used_at(),
        Some(now)
    );
    let audits = service.store.pending_audits().await.unwrap();
    assert_eq!(audits.len(), 2);
    assert_eq!(audits[1].action, TokenAuditAction::Authenticated);
}

#[tokio::test]
async fn workers_merge_usage_for_the_same_token() {
    let queue = Arc::new(MemoryTokenUsageQueue::new(10));
    let service = UsageQueueFixture::service(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let first = Utc::now();
    let second = first + Duration::seconds(1);

    service
        .authenticate(token.id(), token.secret(), None, first)
        .await
        .unwrap();
    service
        .authenticate(token.id(), token.secret(), None, second)
        .await
        .unwrap();
    assert_eq!(queue.len(), 2);

    let worker = UsageQueueFixture::worker(&service, queue.clone(), 100).await;
    assert_eq!(worker.drain().await.unwrap(), 2);

    assert_eq!(
        service
            .store
            .get(token.id())
            .await
            .unwrap()
            .unwrap()
            .metadata()
            .last_used_at(),
        Some(second)
    );
    assert_eq!(service.store.pending_audits().await.unwrap().len(), 3);
}

#[tokio::test]
async fn failed_authentication_queues_the_audit_event() {
    let queue = Arc::new(MemoryTokenUsageQueue::new(10));
    let service = UsageQueueFixture::service(queue.clone());

    assert!(
        service
            .authenticate("missing", "wrong-secret", None, Utc::now())
            .await
            .is_err()
    );
    assert_eq!(queue.len(), 1);
    assert!(service.store.pending_audits().await.unwrap().is_empty());

    let worker = UsageQueueFixture::worker(&service, queue, 100).await;
    worker.drain().await.unwrap();

    let audits = service.store.pending_audits().await.unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].action, TokenAuditAction::AuthenticationFailed);
}

#[tokio::test]
async fn signing_queues_both_audit_events() {
    let queue = Arc::new(MemoryTokenUsageQueue::new(10));
    let service = UsageQueueFixture::service(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();
    assert_eq!(queue.len(), 1);

    let worker = UsageQueueFixture::worker(&service, queue, 100).await;
    worker.drain().await.unwrap();

    let actions: Vec<TokenAuditAction> = service
        .store
        .pending_audits()
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.action)
        .collect();
    assert_eq!(
        actions,
        vec![
            TokenAuditAction::Issued,
            TokenAuditAction::Authenticated,
            TokenAuditAction::Signed,
        ]
    );
}

#[tokio::test]
async fn queue_saturation_fails_closed() {
    let queue = Arc::new(MemoryTokenUsageQueue::new(0));
    let service = UsageQueueFixture::service(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    assert!(
        service
            .authenticate(token.id(), token.secret(), None, Utc::now())
            .await
            .is_err()
    );
    assert!(queue.is_empty());
    assert!(
        service
            .store
            .get(token.id())
            .await
            .unwrap()
            .unwrap()
            .metadata()
            .last_used_at()
            .is_none()
    );
}

#[tokio::test]
async fn releasing_a_reservation_makes_it_available_again() {
    let queue = MemoryTokenUsageQueue::new(10);
    let event = crate::TokenAuditEvent::new(TokenAuditAction::Authenticated, Utc::now())
        .with_token("token-a", Some("owner-a"));
    queue.enqueue(TokenUsage::audit(event)).await.unwrap();

    let reservation = queue.reserve(1).await.unwrap().pop().unwrap();
    queue.release(&reservation).await.unwrap();
    let retried = queue.reserve(1).await.unwrap().pop().unwrap();

    assert_eq!(retried.id(), reservation.id());
    queue.acknowledge(&retried).await.unwrap();
    assert!(queue.is_empty());
}
