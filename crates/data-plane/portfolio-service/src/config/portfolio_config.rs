use super::{AuthConfig, FreshnessConfig, OrderRequestConfig, OwnershipConfig, UpdateConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_exchange_gateway::connector::PortfolioConnectorConfig;
use platform_runtime::Config;

#[derive(Debug, Clone)]
pub struct PortfolioConfig {
    log_filter: String,
    order_request: OrderRequestConfig,
    auth: AuthConfig,
    ownership: OwnershipConfig,
    update: UpdateConfig,
    freshness: FreshnessConfig,
    connector: PortfolioConnectorConfig,
}

impl PortfolioConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            log_filter: Config::log_filter(vars)?,
            order_request: OrderRequestConfig::from_vars(vars)?,
            auth: AuthConfig::from_vars(vars)?,
            ownership: OwnershipConfig::from_vars(vars)?,
            update: UpdateConfig::from_vars(vars)?,
            freshness: FreshnessConfig::from_vars(vars)?,
            connector: PortfolioConnectorConfig::from_vars(vars)?,
        })
    }

    pub fn log_filter(&self) -> &str {
        &self.log_filter
    }

    pub fn order_request(&self) -> &OrderRequestConfig {
        &self.order_request
    }

    pub fn auth(&self) -> &AuthConfig {
        &self.auth
    }

    pub fn ownership(&self) -> &OwnershipConfig {
        &self.ownership
    }

    pub fn update(&self) -> &UpdateConfig {
        &self.update
    }

    pub fn freshness(&self) -> FreshnessConfig {
        self.freshness
    }

    pub fn connector(&self) -> &PortfolioConnectorConfig {
        &self.connector
    }
}
