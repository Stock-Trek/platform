use crate::connector::portfolio::AccountEventStream;
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{AccountSnapshot, ConnectRequest, PortfolioAuth, WsConnection},
};
use std::time::Duration;
use stock_trek_types::cex::cex_id::CexId;

pub trait PortfolioEventStream: AccountEventStream + WsConnection {}

impl<T> PortfolioEventStream for T where T: AccountEventStream + WsConnection + Send {}

#[async_trait]
pub trait PortfolioTransport: Send + Sync {
    fn cex_id(&self) -> CexId;

    fn reconnect_backoff(&self) -> Duration;

    fn connect_request(&self) -> BuildResult<ConnectRequest>;

    async fn portfolio_auth(&self, credentials: &ApiKeyCredentials) -> BuildResult<PortfolioAuth>;

    async fn account_snapshot(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot>;

    async fn open_stream(
        &self,
        request: ConnectRequest,
    ) -> BuildResult<Box<dyn PortfolioEventStream>>;
}
