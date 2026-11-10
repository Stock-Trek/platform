use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;

mod auth;
mod order_request;

pub use auth::AuthConfig;
pub use order_request::OrderRequestConfig;

#[derive(Debug, Clone)]
pub struct PortfolioConfig {
    log_filter: String,
    order_request: OrderRequestConfig,
    auth: AuthConfig,
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
}
