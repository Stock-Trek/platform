use crate::ApiToken;
use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait TokenStore: Send + Sync {
    async fn put(&self, token: &ApiToken) -> BpResult<()>;

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>>;

    async fn delete(&self, id: &str) -> BpResult<()>;

    async fn ids(&self) -> BpResult<Vec<String>>;

    /// Atomically replaces `expected` with `token` when the stored token still
    /// matches `expected`. Stores must reject the write with a conflict error
    /// when the current value has changed.
    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()>;
}
