use super::AccountEventStream;
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_types::{api_key_credential::ApiKeyCredentials, portfolio::AccountSnapshot};
use std::time::Duration;
use stock_trek_types::cex::cex_id::CexId;

#[async_trait]
pub trait PortfolioTransport: Send + Sync {
    fn cex_id(&self) -> CexId;

    fn refresh_interval(&self) -> Duration;

    fn reconnect_backoff(&self) -> Duration;

    async fn create_listen_key(&self, credentials: &ApiKeyCredentials) -> BuildResult<String>;

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BuildResult<()>;

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BuildResult<()>;

    async fn account_snapshot(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot>;

    async fn open_stream(&self, listen_key: &str) -> BuildResult<Box<dyn AccountEventStream>>;
}
