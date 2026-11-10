use crate::{
    ApiToken, TokenAuditEvent, TokenMutation, TokenRotationTransaction, VaultTokenStoreConfig,
    token_store::TokenStore,
    vault_client::{VaultClient, WriteCondition},
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};

pub struct VaultTokenStore {
    client: VaultClient,
}

impl VaultTokenStore {
    pub fn new(config: VaultTokenStoreConfig) -> BpResult<Self> {
        Ok(Self {
            client: VaultClient::new(config)?,
        })
    }
}

#[async_trait]
impl TokenStore for VaultTokenStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.client.put(token, WriteCondition::Create).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.client.get(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        self.client.delete(id).await
    }

    async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        self.client.delete_if(expected).await
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.client.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        let Some((current, version)) = self.client.get_entry(token.id()).await? else {
            return Err(BpError::unknown_token(token.id()));
        };
        if current.to_value() != expected.to_value() {
            return Err(BpError::token_conflict(token.id()));
        }
        self.client
            .put(token, WriteCondition::Update(version))
            .await
    }

    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent) -> BpResult<bool> {
        self.client.commit(mutation, event).await
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
