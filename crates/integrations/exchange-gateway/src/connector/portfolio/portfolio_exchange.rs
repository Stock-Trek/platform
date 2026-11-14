use crate::connector::portfolio::PortfolioStream;
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{AccountSnapshot, Credential, PortfolioAuth},
};
use std::time::Duration;
use stock_trek_types::cex::cex_id::CexId;

#[async_trait]
pub trait PortfolioExchange: Send + Sync {
    fn cex_id(&self) -> CexId;

    fn reconnect_backoff(&self) -> Duration;

    async fn portfolio_auth(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> PlatformResult<PortfolioAuth>;

    async fn account_snapshot(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> PlatformResult<AccountSnapshot>;

    async fn open_stream(
        &self,
        auth: &PortfolioAuth,
        credential: Option<&Credential>,
    ) -> PlatformResult<PortfolioStream>;
}
