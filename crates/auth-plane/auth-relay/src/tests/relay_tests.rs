use crate::{AuthRelay, AuthRelayConfig, AuthRelayRunner};
use async_trait::async_trait;
use chrono::Utc;
use platform_auth_service::{
    MemoryTokenStore, MemoryTokenUsageQueue, TokenAuditAction, TokenAuditEvent, TokenAuditLog,
    TokenStore, TokenUsageQueueConfig, VaultTokenStoreConfig,
};
use platform_errors::{BuildError, BuildResult};
use platform_time::timeouts::Timeouts;
use std::sync::{
    Arc, Mutex,
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
        self.events.lock().expect("audit lock").clone()
    }
}

#[async_trait]
impl TokenAuditLog for ToggleAudit {
    async fn record(&self, event: TokenAuditEvent) -> BuildResult<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(BuildError::auth_store(
                "audit",
                "relay-test",
                true,
                "sink unavailable".to_string(),
            ));
        }
        self.events.lock().expect("audit lock").push(event);
        Ok(())
    }
}

struct RelayFixture;

impl RelayFixture {
    async fn store() -> Arc<MemoryTokenStore> {
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

    fn config() -> AuthRelayConfig {
        AuthRelayConfig {
            log_filter: "info".to_string(),
            vault: VaultTokenStoreConfig::new(
                "https://vault.example.com",
                "vault-token",
                "platform-tokens",
                Duration::from_secs(10),
            )
            .expect("valid vault config"),
            audit_queue_url: "https://sqs.example.com/123/audit".to_string(),
            audit_timeout: Duration::from_secs(5),
            aws_timeouts: Timeouts::from_durations(Duration::from_secs(5), Duration::from_secs(15)),
            usage_queue: TokenUsageQueueConfig::new(
                "redis://valkey.example.com:6379",
                "auth-usage",
                Duration::from_secs(5),
                1000,
                100,
            )
            .expect("valid usage queue config"),
        }
    }
}

#[tokio::test]
async fn drains_pending_events_after_a_later_successful_attempt() {
    let store = RelayFixture::store().await;
    let audit = Arc::new(ToggleAudit::new());
    audit.fail();
    let relay = AuthRelay::new(
        store.clone(),
        audit.clone(),
        RelayFixture::usage_queue(),
        100,
    );

    assert!(relay.drain().await.is_err());
    assert_eq!(store.pending_audits().await.unwrap().len(), 1);

    audit.succeed();
    assert_eq!(relay.drain().await.unwrap(), 1);
    assert!(store.pending_audits().await.unwrap().is_empty());
    assert_eq!(audit.events().len(), 1);
    assert_eq!(audit.events()[0].event_id(), Some("event-a"));
}

#[tokio::test]
async fn runner_drains_pending_events_in_a_single_pass() {
    let store = RelayFixture::store().await;
    let audit = Arc::new(ToggleAudit::new());
    let runner = AuthRelayRunner::new(
        RelayFixture::config(),
        AuthRelay::new(
            store.clone(),
            audit.clone(),
            RelayFixture::usage_queue(),
            100,
        ),
    );

    runner.run().await.expect("single relay pass");

    assert!(store.pending_audits().await.unwrap().is_empty());
    assert_eq!(audit.events().len(), 1);
}

#[tokio::test]
async fn runner_returns_delivery_failures_so_the_cron_job_can_retry() {
    let store = RelayFixture::store().await;
    let audit = Arc::new(ToggleAudit::new());
    audit.fail();
    let runner = AuthRelayRunner::new(
        RelayFixture::config(),
        AuthRelay::new(
            store.clone(),
            audit.clone(),
            RelayFixture::usage_queue(),
            100,
        ),
    );

    assert!(runner.run().await.is_err());

    assert_eq!(store.pending_audits().await.unwrap().len(), 1);
}
