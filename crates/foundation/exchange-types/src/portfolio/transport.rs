use super::{AccountEventStream, AccountSnapshot};
use crate::api_key_credential::ApiKeyCredentials;
use async_trait::async_trait;
use platform_errors::BpResult;
use std::time::Duration;
use stock_trek_types::cex::cex_id::CexId;

#[async_trait]
pub trait PortfolioTransport: Send + Sync {
    fn cex_id(&self) -> CexId;

    fn refresh_interval(&self) -> Duration;

    fn reconnect_backoff(&self) -> Duration;

    async fn create_listen_key(&self, credentials: &ApiKeyCredentials) -> BpResult<String>;

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()>;

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()>;

    async fn account_snapshot(&self, credentials: &ApiKeyCredentials) -> BpResult<AccountSnapshot>;

    async fn open_stream(&self, listen_key: &str) -> BpResult<Box<dyn AccountEventStream>>;
}
