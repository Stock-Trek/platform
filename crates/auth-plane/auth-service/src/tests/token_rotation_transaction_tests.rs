use super::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{
    ApiToken, MemoryTokenStore, TokenAuditAction, TokenAuditEvent, TokenAuditLog, TokenMutation,
    TokenRotationTransaction, TokenStore,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use platform_errors::BpResult;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

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
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(platform_errors::BpError::auth_store(
                "audit",
                "rotation-test",
                true,
                "sink unavailable".to_string(),
            ));
        }
        self.events.lock().expect("audit lock").push(event);
        Ok(())
    }
}

struct RotationFixture;

impl RotationFixture {
    fn transaction(
        original: ApiToken,
        replacement: ApiToken,
        now: chrono::DateTime<Utc>,
    ) -> TokenRotationTransaction {
        let mut retired = original.clone();
        retired
            .metadata
            .begin_rotation(replacement.id(), now + chrono::Duration::seconds(60));
        let events = vec![
            TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner())),
            TokenAuditEvent::new(TokenAuditAction::Issued, now)
                .with_token(replacement.id(), Some(replacement.owner())),
        ];
        TokenRotationTransaction::new(original, retired, replacement, events).unwrap()
    }

    fn replacement(original: &ApiToken, id: &str, now: chrono::DateTime<Utc>) -> ApiToken {
        let mut replacement =
            ApiToken::generate(id, original.owner(), now, TestFixture::metadata()).unwrap();
        replacement.metadata.rotated_from = Some(original.id().to_string());
        replacement
    }
}

#[tokio::test]
async fn a_pending_rotation_survives_an_audit_failure_and_retries_both_events() {
    let audit = Arc::new(ToggleAudit::new());
    let service = AuthServiceBuilder::new().build();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    TestFixture::drain(&service, audit.clone()).await;
    audit.fail();
    let now = Utc::now();

    let replacement = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();

    assert_eq!(replacement.id(), "token-b");
    assert_eq!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by(),
        Some("token-b")
    );
    let pending = service.store.pending_rotations().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        pending[0]
            .events()
            .iter()
            .all(|event| event.event_id().is_some())
    );

    audit.succeed();
    assert_eq!(TestFixture::drain(&service, audit.clone()).await, 2);
    assert!(service.store.pending_rotations().await.unwrap().is_empty());
    let actions: Vec<_> = audit.events().iter().map(|event| event.action).collect();
    assert_eq!(
        actions,
        vec![
            TokenAuditAction::Issued,
            TokenAuditAction::Rotated,
            TokenAuditAction::Issued
        ]
    );
    assert_eq!(TestFixture::drain(&service, audit.clone()).await, 0);
}

#[tokio::test]
async fn materializing_a_transaction_twice_is_idempotent() {
    let store = MemoryTokenStore::default();
    let now = Utc::now();
    let original = TestFixture::token("token-a");
    store.put(&original).await.unwrap();
    let replacement = RotationFixture::replacement(&original, "token-b", now);
    let transaction = RotationFixture::transaction(original, replacement, now);

    store.commit_rotation(&transaction).await.unwrap();
    store.materialize_rotation(&transaction).await.unwrap();
    store.materialize_rotation(&transaction).await.unwrap();

    let retired = store.get("token-a").await.unwrap().unwrap();
    assert_eq!(retired.metadata().replaced_by(), Some("token-b"));
    assert!(store.get("token-b").await.unwrap().is_some());
    let pending = store.pending_rotations().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].events(), transaction.events());

    store
        .acknowledge_rotation(transaction.transaction_id())
        .await
        .unwrap();
    assert!(store.pending_rotations().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_transaction_mutation_and_its_event_share_one_commit() {
    let store = MemoryTokenStore::default();
    let now = Utc::now();
    let token = TestFixture::token("token-a");
    let event = TokenAuditEvent::new(TokenAuditAction::Issued, now)
        .with_token(token.id(), Some(token.owner()));

    let applied = store
        .commit(TokenMutation::Create(token.clone()), event)
        .await
        .unwrap();

    assert!(applied);
    assert_eq!(
        store.get(token.id()).await.unwrap().unwrap().id(),
        token.id()
    );
    let pending = store.pending_audits().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].action, TokenAuditAction::Issued);
    assert!(pending[0].event_id().is_some());
}

#[tokio::test]
async fn rotation_transactions_assign_event_ids() {
    let now = Utc::now();
    let original = TestFixture::token("token-a");
    let mut retired = original.clone();
    retired
        .metadata
        .begin_rotation("token-b", now + chrono::Duration::seconds(60));
    let mut replacement =
        ApiToken::generate("token-b", original.owner(), now, TestFixture::metadata()).unwrap();
    replacement.metadata.rotated_from = Some(original.id().to_string());
    let events = vec![
        TokenAuditEvent::new(TokenAuditAction::Rotated, now),
        TokenAuditEvent::new(TokenAuditAction::Issued, now),
    ];

    let transaction =
        crate::TokenRotationTransaction::new(original, retired, replacement, events).unwrap();

    assert!(
        transaction
            .events()
            .iter()
            .all(|event| event.event_id().is_some())
    );
    assert!(
        transaction
            .transaction_id()
            .starts_with(|c: char| c.is_ascii_digit())
    );
}

#[test]
fn rotation_ids_are_stable_across_retries() {
    let now = Utc::now();
    let original = TestFixture::token("token-a");
    let replacement = RotationFixture::replacement(&original, "token-b", now);
    let first = RotationFixture::transaction(original.clone(), replacement.clone(), now);
    let retry = RotationFixture::transaction(original, replacement, now + Duration::seconds(30));

    assert_eq!(first.transaction_id(), retry.transaction_id());
    let first_ids: Vec<_> = first
        .events()
        .iter()
        .map(|event| event.event_id())
        .collect();
    let retry_ids: Vec<_> = retry
        .events()
        .iter()
        .map(|event| event.event_id())
        .collect();
    assert_eq!(first_ids, retry_ids);
    assert_ne!(first_ids[0], first_ids[1]);
}

#[test]
fn rotation_storage_keys_cannot_collide_with_token_ids() {
    let key = format!(
        "{}transaction-id",
        crate::token_rotation_transaction::ROTATION_TRANSACTION_PREFIX
    );

    assert!(ApiToken::validate_id(&key).is_err());
}
