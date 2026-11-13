pub use crate::portfolio::{
    AuthBinder, AuthError, Credential, CredentialProvider, PortfolioAuth, PortfolioIrisClient,
    RenewalSchedule, SessionLifecycle,
};
use crate::{
    api_key_credential::ApiKeyCredentials,
    exchange::ETExchange,
    http::HttpClient,
    portfolio::{AccountEvent, AccountSnapshot},
    request::ETHttpRequest,
};
use platform_errors::BuildResult;
use std::{sync::Arc, time::Duration};
use stock_trek_types::cex::asset_id::AssetId;

pub struct PortfolioAuthContext {
    credentials: ApiKeyCredentials,
    http: Arc<dyn HttpClient>,
    rest_url: String,
    refresh_interval: Duration,
}

impl PortfolioAuthContext {
    pub fn new(
        credentials: ApiKeyCredentials,
        http: Arc<dyn HttpClient>,
        rest_url: impl Into<String>,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            credentials,
            http,
            rest_url: rest_url.into(),
            refresh_interval,
        }
    }

    pub fn credentials(&self) -> &ApiKeyCredentials {
        &self.credentials
    }

    pub fn http(&self) -> Arc<dyn HttpClient> {
        Arc::clone(&self.http)
    }

    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }
}

pub trait ETExchangePortfolio: ETExchange {
    type SnapshotRequest: ETHttpRequest<Exchange = Self> + Send;

    fn account_snapshot_request() -> Self::SnapshotRequest;
    fn account_snapshot(
        response: <Self::SnapshotRequest as ETHttpRequest>::Response,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<AccountSnapshot>;
    fn account_event(
        value: &serde_json::Value,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<Option<AccountEvent>>;
    fn portfolio_auth(&self, context: PortfolioAuthContext) -> BuildResult<PortfolioAuth>;
}
