pub use crate::portfolio::{
    AuthBinder, AuthError, Credential, CredentialProvider, PortfolioAuth, PortfolioIrisClient,
    RenewalSchedule, SessionLifecycle,
};
use crate::{
    api_key_credential::ApiKeyCredentials, exchange_account_snapshot::ETExchangeAccountSnapshot,
    http::HttpClient,
};
use platform_errors::BuildResult;
use std::{sync::Arc, time::Duration};

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

pub trait ETExchangePortfolio: ETExchangeAccountSnapshot {
    fn portfolio_auth(&self, context: PortfolioAuthContext) -> BuildResult<PortfolioAuth>;
}
