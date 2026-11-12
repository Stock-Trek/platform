use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use crate::tests::token_usage_queue_fixture_tests::ToggleTokenUsageQueue;
use crate::{MemoryTokenUsageQueue, TokenAuditAction, TokenUsageWorker};
use chrono::Utc;
use std::sync::Arc;

#[tokio::test]
async fn signature_verification_queues_verified_usage_after_reserving_the_replay() {
    let queue = Arc::new(MemoryTokenUsageQueue::new(1024));
    let service = TestFixture::service_with_usage(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let worker = TokenUsageWorker::new(service.store.clone(), queue.clone(), 100);
    assert_eq!(worker.drain().await.expect("signing usage"), 1);
    assert!(
        service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("verification completes")
            .is_valid()
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(worker.drain().await.expect("verification usage"), 1);

    let actions: Vec<TokenAuditAction> = service
        .store
        .pending_audits()
        .await
        .expect("pending audits")
        .into_iter()
        .map(|event| event.action)
        .collect();
    assert_eq!(
        actions,
        vec![
            TokenAuditAction::Issued,
            TokenAuditAction::Authenticated,
            TokenAuditAction::Signed,
            TokenAuditAction::SignatureVerified,
        ]
    );
}

#[tokio::test]
async fn a_failed_usage_enqueue_releases_the_replay_reservation() {
    let queue = Arc::new(ToggleTokenUsageQueue::new());
    let service = TestFixture::service_with_usage(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    queue.fail();

    let verification = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("queue failure is classified");

    assert!(verification.is_retryable());

    queue.succeed();
    assert!(
        service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("retried verification")
            .is_valid()
    );
}

#[tokio::test]
async fn a_failed_usage_enqueue_does_not_queue_verified_usage() {
    let queue = Arc::new(ToggleTokenUsageQueue::new());
    let service = TestFixture::service_with_usage(queue.clone());
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    queue.fail();

    assert!(
        service
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("queue failure is classified")
            .is_retryable()
    );

    queue.succeed();
    let worker = TokenUsageWorker::new(service.store.clone(), queue, 100);
    worker.drain().await.expect("signing usage");

    let actions: Vec<TokenAuditAction> = service
        .store
        .pending_audits()
        .await
        .expect("pending audits")
        .into_iter()
        .map(|event| event.action)
        .collect();
    assert!(!actions.contains(&TokenAuditAction::SignatureVerified));
}
