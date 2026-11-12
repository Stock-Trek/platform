use super::{
    support_tests::TestFixture, vault_kv_server_tests::MockVault,
    vault_kv_state_tests::CapturedRequest,
};
use crate::{
    TokenStore, VaultTokenStore, VaultTokenStoreConfig,
    vault_client::{VaultClient, WriteCondition},
};
use chrono::Utc;
use platform_errors::{AuthError, BuildError};
use secrecy::SecretString;
use std::time::Duration;

struct VaultClientFixture {
    server: MockVault,
    store: VaultTokenStore,
    client: VaultClient,
}

impl VaultClientFixture {
    fn new() -> Self {
        let server = MockVault::start("platform-tokens");
        let config = VaultTokenStoreConfig::new(
            server.url(),
            SecretString::from("vault-token"),
            "platform-tokens",
            Duration::from_secs(5),
        )
        .unwrap();
        let store = VaultTokenStore::new(config.clone()).unwrap();
        let client = VaultClient::new(config).unwrap();
        Self {
            server,
            store,
            client,
        }
    }

    fn cas(request: &CapturedRequest) -> Option<u64> {
        request
            .body
            .pointer("/options/cas")
            .and_then(serde_json::Value::as_u64)
    }
}

#[tokio::test]
async fn writes_and_reads_kv_v2_payloads_with_versions() {
    let fixture = VaultClientFixture::new();
    let token = TestFixture::token("token-a");

    fixture.store.put(&token).await.unwrap();
    let (stored, version) = fixture.client.get_entry("token-a").await.unwrap().unwrap();

    assert_eq!(stored.to_value(), token.to_value());
    assert_eq!(version, 1);
    assert_eq!(
        fixture.server.current("token-a").unwrap().0,
        token.to_value()
    );
    let requests = fixture.server.requests();
    let write = requests
        .iter()
        .find(|request| {
            request.method == "PUT" && request.path == "/v1/platform-tokens/data/token-a"
        })
        .unwrap();
    assert_eq!(write.token.as_deref(), Some("vault-token"));
    assert_eq!(VaultClientFixture::cas(write), Some(0));
    assert_eq!(write.body.get("data"), Some(&token.to_value()));
    let read = requests
        .iter()
        .find(|request| {
            request.method == "GET" && request.path == "/v1/platform-tokens/data/token-a"
        })
        .unwrap();
    assert_eq!(read.path, "/v1/platform-tokens/data/token-a");
    assert!(requests.iter().any(|request| {
        request.method == "PUT" && request.path.contains("/data/index/owners/")
    }));
}

#[tokio::test]
async fn create_is_cas_zero_and_duplicates_map_to_token_exists() {
    let fixture = VaultClientFixture::new();
    let token = TestFixture::token("token-a");

    fixture.store.put(&token).await.unwrap();
    let error = fixture.store.put(&token).await.unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::TokenExists { .. })
    ));
    assert_eq!(fixture.server.current("token-a").unwrap().1, 1);
    let requests = fixture.server.requests();
    let duplicate = requests
        .iter()
        .rev()
        .find(|request| {
            request.method == "PUT" && request.path == "/v1/platform-tokens/data/token-a"
        })
        .unwrap();
    assert_eq!(VaultClientFixture::cas(duplicate), Some(0));
}

#[tokio::test]
async fn update_uses_the_read_version_and_rejects_stale_cas() {
    let fixture = VaultClientFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();

    let now = Utc::now();
    let mut updated = token.clone();
    updated.metadata.record_use(now);
    fixture.store.update(&token, &updated).await.unwrap();

    let (stored, version) = fixture.client.get_entry("token-a").await.unwrap().unwrap();
    assert_eq!(version, 2);
    assert_eq!(stored.metadata().last_used_at(), Some(now));
    let requests = fixture.server.requests();
    let update = requests
        .iter()
        .find(|request| request.method == "PUT" && VaultClientFixture::cas(request) == Some(1))
        .unwrap();
    assert_eq!(update.path, "/v1/platform-tokens/data/token-a");

    let error = fixture
        .client
        .put(&token, WriteCondition::Update(1))
        .await
        .unwrap_err();

    assert!(matches!(error, BuildError::Auth(AuthError::Conflict { .. })));
    assert_eq!(fixture.server.current("token-a").unwrap().1, 2);
    assert_eq!(
        fixture.server.current("token-a").unwrap().0,
        updated.to_value()
    );
}

#[tokio::test]
async fn delete_metadata_removes_all_versions_and_is_idempotent() {
    let fixture = VaultClientFixture::new();
    let token = TestFixture::token("token-a");
    fixture.store.put(&token).await.unwrap();

    fixture.store.delete("token-a").await.unwrap();
    assert!(fixture.store.get("token-a").await.unwrap().is_none());
    assert!(fixture.server.current("token-a").is_none());
    fixture.store.delete("token-a").await.unwrap();

    let requests = fixture.server.requests();
    let deletes: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "DELETE")
        .collect();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[0].path, "/v1/platform-tokens/metadata/token-a");
}

#[tokio::test]
async fn list_uses_metadata_endpoint_and_filters_invalid_keys() {
    let fixture = VaultClientFixture::new();
    for key in ["token-b", "nested/", "bad id", "token-a"] {
        fixture.server.seed(key);
    }

    let ids = fixture.store.ids().await.unwrap();

    assert_eq!(ids, vec!["token-a", "token-b"]);
    let requests = fixture.server.requests();
    let list = requests.last().unwrap();
    assert_eq!(list.method, "GET");
    assert_eq!(list.path, "/v1/platform-tokens/metadata");
    assert_eq!(list.query.as_deref(), Some("list=true"));
}

#[tokio::test]
async fn empty_list_returns_no_ids() {
    let fixture = VaultClientFixture::new();

    let ids = fixture.store.ids().await.unwrap();

    assert!(ids.is_empty());
    let requests = fixture.server.requests();
    assert_eq!(requests[0].path, "/v1/platform-tokens/metadata");
    assert_eq!(requests[0].query.as_deref(), Some("list=true"));
}
