use crate::{api_key_credential::ApiKeyCredentials, portfolio::AuthError};
use async_trait::async_trait;

#[async_trait]
pub trait BinanceListenKeyClient: Send + Sync {
    async fn create_listen_key(&self, credentials: &ApiKeyCredentials)
    -> Result<String, AuthError>;

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> Result<(), AuthError>;

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> Result<(), AuthError>;
}
