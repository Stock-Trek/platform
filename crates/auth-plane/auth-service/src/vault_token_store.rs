use crate::{ApiToken, VaultTokenStoreConfig, token_store::TokenStore, vault_client::VaultClient};
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
}

#[async_trait]
impl TokenStore for VaultTokenStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.client.put(token, true).await
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

    async fn update(&self, token: &ApiToken) -> BpResult<()> {
        self.client.put(token, false).await
    }
}
