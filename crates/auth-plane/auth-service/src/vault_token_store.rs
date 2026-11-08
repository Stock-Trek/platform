use crate::{
    ApiToken, VaultTokenStoreConfig,
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
}
