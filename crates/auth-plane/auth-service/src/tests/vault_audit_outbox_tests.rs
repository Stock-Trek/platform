use super::{
    support_tests::TestFixture, vault_kv_server_tests::MockVault,
    vault_kv_state_tests::CapturedRequest,
};
use crate::{
    TokenAuditAction, TokenAuditEvent, TokenStore, VaultTokenStore, VaultTokenStoreConfig,
    token_audit_outbox::TokenAuditOutbox,
    vault_client::{VaultClient, WriteCondition},
};
use chrono::Utc;
use platform_errors::{AuthError, BpError};
use secrecy::SecretString;
use serde_json::json;
use std::time::Duration;

struct VaultOutboxFixture {
    server: MockVault,
    store: VaultTokenStore,
}

impl VaultOutboxFixture {
    fn new() -> Self {
        let server = MockVault::start("platform-tokens");
        Self {
            store: VaultTokenStore::new(Self::config(&server)).unwrap(),
            server,
        }
    }

    fn config(server: &MockVault) -> VaultTokenStoreConfig {
        VaultTokenStoreConfig::new(
            server.url(),
            SecretString::from("vault-token"),
            "platform-tokens",
            Duration::from_secs(5),
        )
        .unwrap()
    }

    fn client(&self) -> VaultClient {
        VaultClient::new(Self::config(&self.server)).unwrap()
    }

    fn event(action: TokenAuditAction, token_id: &str) -> TokenAuditEvent {
        TokenAuditEvent::new(action, Utc::now()).with_token(token_id, Some("owner-a"))
    }

    fn token_writes(requests: &[CapturedRequest], id: &str) -> usize {
        let path = format!("/v1/platform-tokens/data/{id}");
        requests
            .iter()
            .filter(|request| request.method == "PUT" && request.path == path)
            .count()
    }
}

#[tokio::test]
async fn updating_a_token_preserves_pending_audit_events() {
    let fixture = VaultOutboxFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();
    fixture
        .store
        .append_audit(VaultOutboxFixture::event(
            TokenAuditAction::Authenticated,
            token.id(),
        ))
        .await
        .unwrap();

    let mut updated = token.clone();
    updated.metadata.record_use(Utc::now());
    fixture.store.update(&token, &updated).await.unwrap();

    let pending = fixture.store.pending_audits().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].action, TokenAuditAction::Authenticated);
    assert_eq!(
        fixture
            .store
            .get(token.id())
            .await
            .unwrap()
            .unwrap()
            .to_value(),
        updated.to_value()
    );
}

#[tokio::test]
async fn deleting_a_token_keeps_pending_events_until_they_are_acknowledged() {
    let fixture = VaultOutboxFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();
    let event = VaultOutboxFixture::event(TokenAuditAction::Revoked, token.id())
        .with_event_id("event-revoked");
    fixture.store.append_audit(event).await.unwrap();

    assert!(fixture.store.delete_if(&token).await.unwrap());
    assert!(fixture.store.get(token.id()).await.unwrap().is_none());
    let pending = fixture.store.pending_audits().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_id(), Some("event-revoked"));
    assert!(fixture.store.put(&token).await.is_err());

    fixture
        .store
        .acknowledge_audit("event-revoked")
        .await
        .unwrap();
    assert!(fixture.store.pending_audits().await.unwrap().is_empty());
    assert!(fixture.server.current(token.id()).is_none());
    fixture.store.put(&token).await.unwrap();
}

#[tokio::test]
async fn corrupt_outbox_entries_are_reported_instead_of_dropped() {
    let fixture = VaultOutboxFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();
    let client = fixture.client();
    let (data, version) = client.get_value_entry(token.id()).await.unwrap().unwrap();
    let mut corrupt = data;
    corrupt["audit_outbox"] = json!("not-an-array");
    client
        .put_entry(token.id(), corrupt, WriteCondition::Update(version))
        .await
        .unwrap();

    let writes_before = VaultOutboxFixture::token_writes(&fixture.server.requests(), token.id());
    for result in [
        fixture.store.update(&token, &token).await,
        fixture.store.pending_audits().await.map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(BpError::Auth(AuthError::Store {
                retryable: false,
                ..
            }))
        ));
    }
    assert_eq!(
        VaultOutboxFixture::token_writes(&fixture.server.requests(), token.id()),
        writes_before
    );
}

#[tokio::test]
async fn standalone_audit_events_do_not_consume_token_ids() {
    let fixture = VaultOutboxFixture::new();
    let event =
        TokenAuditEvent::new(TokenAuditAction::Signed, Utc::now()).with_event_id("event-signed");
    fixture.store.append_audit(event).await.unwrap();

    let key = TokenAuditOutbox::storage_key("event-signed");
    assert!(fixture.server.current(&key).is_some());
    let pending = fixture.store.pending_audits().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].action, TokenAuditAction::Signed);
    assert!(fixture.store.ids().await.unwrap().is_empty());

    fixture
        .store
        .acknowledge_audit("event-signed")
        .await
        .unwrap();
    assert!(fixture.server.current(&key).is_none());
    assert!(fixture.store.pending_audits().await.unwrap().is_empty());
}
