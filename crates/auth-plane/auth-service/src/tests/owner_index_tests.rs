use crate::tests::{
    support_tests::{AuthServiceBuilder, OWNER, TestFixture},
    vault_kv_server_tests::MockVault,
};
use crate::{
    ApiToken, AuthService, MemoryTokenStore, OwnerIndex, OwnerIndexEntry, RevocationReason,
    RevokedBy, TokenAuditEvent, TokenMutation, TokenRotationTransaction, TokenStore,
    VaultTokenStore, VaultTokenStoreConfig, vault_client::VaultClient,
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::BuildResult;
use secrecy::SecretString;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct CountingStore {
    inner: MemoryTokenStore,
    ids_calls: AtomicUsize,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            ids_calls: AtomicUsize::new(0),
        }
    }

    fn ids_calls(&self) -> usize {
        self.ids_calls.load(Ordering::SeqCst)
    }

    fn reset_ids_calls(&self) {
        self.ids_calls.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl TokenStore for CountingStore {
    async fn put(&self, token: &ApiToken) -> BuildResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BuildResult<Option<ApiToken>> {
        self.inner.get(id).await
    }

    async fn delete(&self, id: &str) -> BuildResult<()> {
        self.inner.delete(id).await
    }

    async fn delete_if(&self, expected: &ApiToken) -> BuildResult<bool> {
        self.inner.delete_if(expected).await
    }

    async fn ids(&self) -> BuildResult<Vec<String>> {
        self.ids_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.ids().await
    }

    async fn owner_index(&self, owner: &str) -> BuildResult<Option<OwnerIndex>> {
        self.inner.owner_index(owner).await
    }

    async fn put_owner_index(&self, index: &OwnerIndex) -> BuildResult<()> {
        self.inner.put_owner_index(index).await
    }

    async fn owner_indexes(&self) -> BuildResult<Vec<OwnerIndex>> {
        self.inner.owner_indexes().await
    }

    async fn index_owner_token(&self, token: &ApiToken) -> BuildResult<()> {
        self.inner.index_owner_token(token).await
    }

    async fn remove_owner_index_token(&self, owner: &str, id: &str) -> BuildResult<()> {
        self.inner.remove_owner_index_token(owner, id).await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BuildResult<()> {
        self.inner.update(expected, token).await
    }

    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent) -> BuildResult<bool> {
        self.inner.commit(mutation, event).await
    }

    async fn commit_events(
        &self,
        mutation: TokenMutation,
        events: Vec<TokenAuditEvent>,
    ) -> BuildResult<bool> {
        self.inner.commit_events(mutation, events).await
    }

    async fn append_audit(&self, event: TokenAuditEvent) -> BuildResult<()> {
        self.inner.append_audit(event).await
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BuildResult<()> {
        self.inner.commit_rotation(transaction).await
    }

    async fn pending_audits(&self) -> BuildResult<Vec<TokenAuditEvent>> {
        self.inner.pending_audits().await
    }

    async fn acknowledge_audit(&self, event_id: &str) -> BuildResult<()> {
        self.inner.acknowledge_audit(event_id).await
    }

    async fn pending_rotations(&self) -> BuildResult<Vec<TokenRotationTransaction>> {
        self.inner.pending_rotations().await
    }

    async fn acknowledge_rotation(&self, transaction_id: &str) -> BuildResult<()> {
        self.inner.acknowledge_rotation(transaction_id).await
    }
}

struct VaultFixture {
    server: MockVault,
    service: AuthService,
}

impl VaultFixture {
    fn new() -> Self {
        let server = MockVault::start("platform-tokens");
        let config = VaultTokenStoreConfig::new(
            server.url(),
            SecretString::from("vault-token"),
            "platform-tokens",
            Duration::from_secs(5),
        )
        .unwrap();
        let store = Arc::new(VaultTokenStore::new(config).unwrap());
        let service = AuthService::new(
            store.clone(),
            TestFixture::config(),
            Arc::new(crate::MemoryReplayStore::default()),
            TestFixture::immediate_usage(store),
        )
        .unwrap();
        Self { server, service }
    }

    fn client(&self) -> VaultClient {
        let config = VaultTokenStoreConfig::new(
            self.server.url(),
            SecretString::from("vault-token"),
            "platform-tokens",
            Duration::from_secs(5),
        )
        .unwrap();
        VaultClient::new(config).unwrap()
    }

    async fn forget_index(&self, owner: &str) {
        self.client()
            .delete_key(&OwnerIndex::storage_key(owner))
            .await
            .unwrap();
    }
}

struct IndexAssertions;

impl IndexAssertions {
    fn entry_ids(index: &OwnerIndex) -> Vec<&str> {
        index.tokens().iter().map(|entry| entry.id()).collect()
    }
}

#[tokio::test]
async fn owner_signing_reads_the_index_instead_of_scanning_every_token() {
    let store = Arc::new(CountingStore::new());
    let service = AuthServiceBuilder::new().store(store.clone()).build();
    let owner = "indexed-owner";
    TestFixture::add_token(&service, "owner-token", owner).await;
    for number in 0..20 {
        TestFixture::add_token(&service, &format!("other-token-{number}"), "other-owner").await;
    }
    store.reset_ids_calls();

    let selected = service.token_for_owner(owner, Utc::now()).await.unwrap();

    assert_eq!(selected.id(), "owner-token");
    assert_eq!(store.ids_calls(), 0);
}

#[tokio::test]
async fn revoked_owner_selection_does_not_scan_every_token() {
    let store = Arc::new(CountingStore::new());
    let service = AuthServiceBuilder::new().store(store.clone()).build();
    let token = TestFixture::add_token(&service, "owner-token", "revoked-owner").await;
    for number in 0..20 {
        TestFixture::add_token(&service, &format!("other-token-{number}"), "other-owner").await;
    }
    service
        .revoke_token(
            token.id(),
            RevocationReason::OwnerRequested,
            RevokedBy::Owner,
            Utc::now(),
        )
        .await
        .unwrap();
    store.reset_ids_calls();

    let error = service
        .token_for_owner("revoked-owner", Utc::now())
        .await
        .unwrap_err();

    assert!(matches!(error, platform_errors::BuildError::Auth(_)));
    assert_eq!(store.ids_calls(), 0);
}

#[tokio::test]
async fn missing_vault_indexes_are_rebuilt_from_tokens() {
    let fixture = VaultFixture::new();
    TestFixture::add_token(&fixture.service, "token-a", OWNER).await;
    fixture.forget_index(OWNER).await;
    assert!(
        fixture
            .service
            .store
            .owner_index(OWNER)
            .await
            .unwrap()
            .is_none()
    );

    let selected = fixture
        .service
        .token_for_owner(OWNER, Utc::now())
        .await
        .unwrap();

    assert_eq!(selected.id(), "token-a");
    let index = fixture
        .service
        .store
        .owner_index(OWNER)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(IndexAssertions::entry_ids(&index), vec!["token-a"]);
}

#[tokio::test]
async fn empty_vault_indexes_are_rebuilt_from_tokens() {
    let fixture = VaultFixture::new();
    TestFixture::add_token(&fixture.service, "token-a", OWNER).await;
    fixture
        .service
        .store
        .put_owner_index(&OwnerIndex::new(OWNER))
        .await
        .unwrap();

    let tokens = fixture.service.tokens_for_owner(OWNER).await.unwrap();

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id(), "token-a");
    let index = fixture
        .service
        .store
        .owner_index(OWNER)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(IndexAssertions::entry_ids(&index), vec!["token-a"]);
}

#[tokio::test]
async fn stale_vault_index_entries_are_rebuilt_from_tokens() {
    let fixture = VaultFixture::new();
    let token = TestFixture::add_token(&fixture.service, "token-a", OWNER).await;
    let mut used_token = token.clone();
    used_token.metadata.record_use(Utc::now());
    let mut stale_index = OwnerIndex::new(OWNER);
    stale_index.upsert(OwnerIndexEntry::from_token(&used_token));
    fixture
        .service
        .store
        .put_owner_index(&stale_index)
        .await
        .unwrap();

    let selected = fixture
        .service
        .token_for_owner(OWNER, Utc::now())
        .await
        .unwrap();

    assert_eq!(selected.id(), token.id());
    let index = fixture
        .service
        .store
        .owner_index(OWNER)
        .await
        .unwrap()
        .unwrap();
    assert!(index.tokens()[0].matches_token(&token));
}

#[tokio::test]
async fn reconciliation_rebuilds_current_owners_and_empties_stale_indexes() {
    let service = TestFixture::service();
    TestFixture::add_token(&service, "token-b", OWNER).await;
    TestFixture::add_token(&service, "token-a", OWNER).await;
    service
        .store
        .put_owner_index(&OwnerIndex::new(OWNER))
        .await
        .unwrap();
    let stale_token = ApiToken::generate(
        "ghost-token",
        "ghost-owner",
        Utc::now(),
        TestFixture::metadata(),
    )
    .unwrap();
    service
        .store
        .put_owner_index(&OwnerIndex::from_tokens("ghost-owner", &[stale_token]))
        .await
        .unwrap();

    let written = service.reconcile_owner_indexes().await.unwrap();

    let index = service.store.owner_index(OWNER).await.unwrap().unwrap();
    let ghost = service
        .store
        .owner_index("ghost-owner")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(written, 2);
    assert_eq!(
        IndexAssertions::entry_ids(&index),
        vec!["token-a", "token-b"]
    );
    assert!(ghost.is_empty());
}

#[tokio::test]
async fn rotation_updates_the_index_for_both_tokens() {
    let service = TestFixture::service();
    let original = TestFixture::add_token(&service, "token-a", OWNER).await;
    let now = Utc::now();

    let replacement = service.rotate_token("token-a", now).await.unwrap();

    let index = service.store.owner_index(OWNER).await.unwrap().unwrap();
    assert_eq!(
        IndexAssertions::entry_ids(&index),
        vec![original.id(), replacement.id()]
    );
    let retired = service.token(original.id()).await.unwrap();
    assert_eq!(retired.metadata().replaced_by(), Some(replacement.id()));
}
