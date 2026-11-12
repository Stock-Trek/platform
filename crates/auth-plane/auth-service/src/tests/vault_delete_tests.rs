use crate::tests::{
    support_tests::TestFixture, vault_kv_server_tests::MockVault,
    vault_kv_state_tests::CapturedRequest,
};
use crate::{TokenStore, VaultTokenStore, VaultTokenStoreConfig, vault_client::WriteCondition};
use chrono::Utc;
use secrecy::SecretString;
use serde_json::{Value, json};
use std::time::Duration;

struct VaultDeleteFixture {
    server: MockVault,
    store: VaultTokenStore,
}

impl VaultDeleteFixture {
    fn new() -> Self {
        let server = MockVault::start("platform-tokens");
        let config = VaultTokenStoreConfig::new(
            server.url(),
            SecretString::from("vault-token"),
            "platform-tokens",
            Duration::from_secs(5),
        )
        .unwrap();
        Self {
            store: VaultTokenStore::new(config).unwrap(),
            server,
        }
    }

    fn cas(request: &CapturedRequest) -> Option<u64> {
        request.body.pointer("/options/cas").and_then(Value::as_u64)
    }
}

#[tokio::test]
async fn conditional_delete_uses_a_cas_tombstone_then_deletes_metadata() {
    let fixture = VaultDeleteFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();

    assert!(fixture.store.delete_if(&token).await.unwrap());

    assert!(fixture.store.get("token-a").await.unwrap().is_none());
    assert!(fixture.server.current("token-a").is_none());
    let requests = fixture.server.requests();
    let delete_index = requests
        .iter()
        .position(|request| request.method == "DELETE")
        .unwrap();
    let tombstone = requests[..delete_index]
        .iter()
        .rev()
        .find(|request| request.method == "PUT")
        .unwrap();
    assert_eq!(tombstone.path, "/v1/platform-tokens/data/token-a");
    assert_eq!(VaultDeleteFixture::cas(tombstone), Some(1));
    assert_eq!(
        tombstone.body.pointer("/data/__platform_deleted__"),
        Some(&json!(true))
    );
    assert_eq!(
        requests[delete_index].path,
        "/v1/platform-tokens/metadata/token-a"
    );
}

#[tokio::test]
async fn conditional_delete_rejects_a_changed_token_without_writing() {
    let fixture = VaultDeleteFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();
    let mut changed = token.clone();
    changed.metadata.record_use(Utc::now());
    fixture.store.update(&token, &changed).await.unwrap();

    assert!(!fixture.store.delete_if(&token).await.unwrap());

    assert_eq!(
        fixture.server.current("token-a").unwrap().0,
        changed.to_value()
    );
    let requests = fixture.server.requests();
    assert!(!requests.iter().any(|request| request.method == "DELETE"));
    assert_eq!(
        fixture
            .store
            .get("token-a")
            .await
            .unwrap()
            .unwrap()
            .to_value(),
        changed.to_value()
    );
}

#[tokio::test]
async fn conditional_delete_is_idempotent() {
    let fixture = VaultDeleteFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();

    assert!(fixture.store.delete_if(&token).await.unwrap());
    assert!(!fixture.store.delete_if(&token).await.unwrap());

    assert!(fixture.store.get("token-a").await.unwrap().is_none());
    let requests = fixture.server.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        1
    );
}

#[tokio::test]
async fn deleted_ids_can_be_recreated() {
    let fixture = VaultDeleteFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();
    assert!(fixture.store.delete_if(&token).await.unwrap());

    fixture.store.put(&token).await.unwrap();

    assert_eq!(
        fixture
            .store
            .get("token-a")
            .await
            .unwrap()
            .unwrap()
            .to_value(),
        token.to_value()
    );
    assert_eq!(fixture.server.current("token-a").unwrap().1, 1);
}

#[tokio::test]
async fn stale_tombstone_left_by_a_failed_delete_is_cleaned_up() {
    let fixture = VaultDeleteFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();
    let config = VaultTokenStoreConfig::new(
        fixture.server.url(),
        SecretString::from("vault-token"),
        "platform-tokens",
        Duration::from_secs(5),
    )
    .unwrap();
    let client = crate::vault_client::VaultClient::new(config).unwrap();
    client
        .put_entry(
            "token-a",
            json!({ "__platform_deleted__": true }),
            WriteCondition::Update(1),
        )
        .await
        .unwrap();

    assert!(!fixture.store.delete_if(&token).await.unwrap());

    assert!(fixture.server.current("token-a").is_none());
    fixture.store.put(&token).await.unwrap();
    assert!(fixture.store.get("token-a").await.unwrap().is_some());
}
