use crate::{
    MemoryTokenStore, MemoryTokenUsageQueue, TokenAuditAction, TokenAuditEvent, TokenAuditLog,
    TokenStore, TokenUsageQueue, token_drain::TokenDrain, token_usage::TokenUsage,
};
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use platform_errors::{PlatformError, PlatformResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

struct ToggleAudit {
    failing: AtomicBool,
    events: Mutex<Vec<TokenAuditEvent>>,
}

impl ToggleAudit {
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
        self.events.lock().clone()
    }
}

#[async_trait]
impl TokenAuditLog for ToggleAudit {
    async fn record(&self, event: TokenAuditEvent) -> PlatformResult<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(PlatformError::auth_store(
                "audit",
                "drain-test",
                true,
                "sink unavailable".to_string(),
            ));
        }
        self.events.lock().push(event);
        Ok(())
    }
}

struct DrainFixture;

impl DrainFixture {
    async fn store_with_pending_audit() -> Arc<MemoryTokenStore> {
        let store = Arc::new(MemoryTokenStore::default());
        store
            .append_audit(
                TokenAuditEvent::new(TokenAuditAction::Issued, Utc::now())
                    .with_event_id("event-a")
                    .with_token("token-a", Some("owner-a")),
            )
            .await
            .expect("pending audit");
        store
    }

    fn usage_queue() -> Arc<MemoryTokenUsageQueue> {
        Arc::new(MemoryTokenUsageQueue::new(1024))
    }
}

#[tokio::test]
async fn drains_pending_events_after_a_later_successful_attempt() {
    let store = DrainFixture::store_with_pending_audit().await;
    let audit = Arc::new(ToggleAudit::new());
    audit.fail();
    let drain = TokenDrain::new(
        store.clone(),
        audit.clone(),
        DrainFixture::usage_queue(),
        100,
        Duration::from_secs(3600),
    );

    assert!(drain.drain().await.is_err());
    assert_eq!(store.pending_audits().await.unwrap().len(), 1);

    audit.succeed();
    assert_eq!(drain.drain().await.unwrap(), 1);
    assert!(store.pending_audits().await.unwrap().is_empty());
    assert_eq!(audit.events().len(), 1);
    assert_eq!(audit.events()[0].event_id(), Some("event-a"));
}

#[tokio::test]
async fn drains_a_pending_usage_record_and_relays_its_audit_event() {
    let store = Arc::new(MemoryTokenStore::default());
    let audit = Arc::new(ToggleAudit::new());
    let queue = DrainFixture::usage_queue();
    let queue_handle = queue.clone();
    queue
        .enqueue(TokenUsage::audit(
            TokenAuditEvent::new(TokenAuditAction::SignatureVerified, Utc::now())
                .with_event_id("event-b")
                .with_token("token-b", Some("owner-b")),
        ))
        .await
        .expect("enqueue usage");
    let drain = TokenDrain::new(
        store.clone(),
        audit.clone(),
        queue,
        100,
        Duration::from_secs(3600),
    );

    let applied_and_delivered = drain.drain().await.unwrap();

    assert_eq!(applied_and_delivered, 2);
    assert!(queue_handle.is_empty());
    assert!(store.pending_audits().await.unwrap().is_empty());
    assert_eq!(audit.events().len(), 1);
    assert_eq!(audit.events()[0].event_id(), Some("event-b"));
}

#[tokio::test]
async fn returns_delivery_failures_so_the_next_cycle_can_retry() {
    let store = DrainFixture::store_with_pending_audit().await;
    let audit = Arc::new(ToggleAudit::new());
    audit.fail();
    let drain = TokenDrain::new(
        store.clone(),
        audit.clone(),
        DrainFixture::usage_queue(),
        100,
        Duration::from_secs(3600),
    );

    assert!(drain.drain().await.is_err());

    assert_eq!(store.pending_audits().await.unwrap().len(), 1);
}
