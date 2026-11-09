use crate::ApiToken;
use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait TokenStore: Send + Sync {
    async fn put(&self, token: &ApiToken) -> BpResult<()>;

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>>;

    async fn delete(&self, id: &str) -> BpResult<()>;

    async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        let Some(current) = self.get(expected.id()).await? else {
            return Ok(false);
        };
        if current.to_value() != expected.to_value() {
            return Ok(false);
        }
        self.delete(expected.id()).await?;
        Ok(true)
    }

    async fn ids(&self) -> BpResult<Vec<String>>;

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()>;
}
