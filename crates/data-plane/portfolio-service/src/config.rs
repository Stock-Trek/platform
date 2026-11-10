use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioConfig {
    log_filter: String,
}

impl PortfolioConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            log_filter: Config::log_filter(vars)?,
        })
    }

    pub fn log_filter(&self) -> &str {
        &self.log_filter
    }
}
