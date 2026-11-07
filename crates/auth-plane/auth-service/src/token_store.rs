use crate::ApiToken;
use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait TokenStore: Send + Sync {
    async fn put(&self, token: &ApiToken) -> BpResult<()>;

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>>;

    async fn delete(&self, id: &str) -> BpResult<()>;

    async fn ids(&self) -> BpResult<Vec<String>>;

    async fn update(&self, token: &ApiToken) -> BpResult<()>;
}
