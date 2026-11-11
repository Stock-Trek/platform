use crate::{
    ApiToken, OwnerIndex, TokenAuditEvent, TokenMutation, TokenRotationTransaction,
    VaultTokenStoreConfig,
    token_store::TokenStore,
    vault_client::{VaultClient, WriteCondition},
};
use async_trait::async_trait;
use platform_errors::BpResult;

pub struct VaultTokenStore {
    client: VaultClient,
}

impl VaultTokenStore {
    pub fn new(config: VaultTokenStoreConfig) -> BpResult<Self> {
        Ok(Self {
            client: VaultClient::new(config)?,
        })
    }

    async fn sync_token_index(&self, token: &ApiToken) {
        if let Err(error) = self.client.upsert_owner_index_token(token).await {
            tracing::warn!(%error, token_id = token.id(), "failed to update owner index");
        }
    }

    async fn sync_removed_index(&self, owner: &str, id: &str) {
        if let Err(error) = self.client.remove_owner_index_token(owner, id).await {
            tracing::warn!(%error, token_id = id, "failed to remove token from owner index");
        }
    }

    async fn sync_mutation_index(&self, mutation: &TokenMutation) {
        match mutation {
            TokenMutation::Create(token) => self.sync_token_index(token).await,
            TokenMutation::Update { expected, token } => {
                if expected.owner() != token.owner() {
                    self.sync_removed_index(expected.owner(), expected.id())
                        .await;
                }
                self.sync_token_index(token).await;
            }
            TokenMutation::Delete { expected } => {
                self.sync_removed_index(expected.owner(), expected.id())
                    .await;
            }
        }
    }
}

#[async_trait]
impl TokenStore for VaultTokenStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.client.put(token, WriteCondition::Create).await?;
        self.sync_token_index(token).await;
        Ok(())
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.client.get(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        let owner = self
            .client
            .get(id)
            .await?
            .map(|token| token.owner().to_string());
        self.client.delete(id).await?;
        if let Some(owner) = owner {
            self.sync_removed_index(&owner, id).await;
        }
        Ok(())
    }

    async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        let deleted = self.client.delete_if(expected).await?;
        if deleted {
            self.sync_removed_index(expected.owner(), expected.id())
                .await;
        }
        Ok(deleted)
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.client.ids().await
    }

    async fn owner_index(&self, owner: &str) -> BpResult<Option<OwnerIndex>> {
        self.client.owner_index(owner).await
    }

    async fn put_owner_index(&self, index: &OwnerIndex) -> BpResult<()> {
        self.client.put_owner_index(index).await
    }

    async fn owner_indexes(&self) -> BpResult<Vec<OwnerIndex>> {
        self.client.owner_indexes().await
    }

    async fn index_owner_token(&self, token: &ApiToken) -> BpResult<()> {
        self.client.upsert_owner_index_token(token).await
    }

    async fn remove_owner_index_token(&self, owner: &str, id: &str) -> BpResult<()> {
        self.client.remove_owner_index_token(owner, id).await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        self.client.update_if_unchanged(expected, token).await?;
        if expected.owner() != token.owner() {
            self.sync_removed_index(expected.owner(), expected.id())
                .await;
        }
        self.sync_token_index(token).await;
        Ok(())
    }

    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent) -> BpResult<bool> {
        let index_mutation = mutation.clone();
        let applied = self.client.commit(mutation, event).await?;
        if applied {
            self.sync_mutation_index(&index_mutation).await;
        }
        Ok(applied)
    }

    async fn commit_events(
        &self,
        mutation: TokenMutation,
        events: Vec<TokenAuditEvent>,
    ) -> BpResult<bool> {
        let index_mutation = mutation.clone();
        let applied = self.client.commit_events(mutation, events).await?;
        if applied {
            self.sync_mutation_index(&index_mutation).await;
        }
        Ok(applied)
    }

    async fn append_audit(&self, event: TokenAuditEvent) -> BpResult<()> {
        self.client.append_audit(event).await
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        self.client.commit_rotation(transaction).await
    }

    async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        self.client.pending_audits().await
    }

    async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        self.client.acknowledge_audit(event_id).await
    }

    async fn pending_rotations(&self) -> BpResult<Vec<TokenRotationTransaction>> {
        self.client.pending_rotations().await
    }

    async fn acknowledge_rotation(&self, transaction_id: &str) -> BpResult<()> {
        self.client.acknowledge_rotation(transaction_id).await
    }
}
