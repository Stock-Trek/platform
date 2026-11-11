use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;
use std::time::Duration;

const URL: &str = "PORTFOLIO_REGISTRY_NATS_URL";
const CLIENT_NAME: &str = "PORTFOLIO_REGISTRY_NATS_CLIENT_NAME";
const OUTBOX_INTERVAL_SECS: &str = "PORTFOLIO_REGISTRY_OUTBOX_INTERVAL_SECS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryNatsConfig {
    url: String,
    client_name: String,
    outbox_interval: Duration,
}

impl RegistryNatsConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            url: Config::required_nats_url(vars, URL)?,
            client_name: vars.required(CLIENT_NAME)?,
            outbox_interval: vars.required_positive_duration_secs(OUTBOX_INTERVAL_SECS)?,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn client_name(&self) -> &str {
        &self.client_name
    }

    pub fn outbox_interval(&self) -> Duration {
        self.outbox_interval
    }
}
