use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;

mod admin;
mod nats;
mod read;
mod store;

pub use admin::AdminConfig;
pub use nats::RegistryNatsConfig;
pub use read::ReadConfig;
pub use store::RegistryStoreConfig;

#[derive(Debug, Clone)]
pub struct PortfolioRegistryConfig {
    log_filter: String,
    store: RegistryStoreConfig,
    nats: RegistryNatsConfig,
    admin: AdminConfig,
    read: ReadConfig,
}

impl PortfolioRegistryConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            log_filter: Config::log_filter(vars)?,
            store: RegistryStoreConfig::from_vars(vars)?,
            nats: RegistryNatsConfig::from_vars(vars)?,
            admin: AdminConfig::from_vars(vars)?,
            read: ReadConfig::from_vars(vars)?,
        })
    }

    pub fn log_filter(&self) -> &str {
        &self.log_filter
    }

    pub fn store(&self) -> &RegistryStoreConfig {
        &self.store
    }

    pub fn nats(&self) -> &RegistryNatsConfig {
        &self.nats
    }

    pub fn admin(&self) -> &AdminConfig {
        &self.admin
    }

    pub fn read(&self) -> &ReadConfig {
        &self.read
    }
}
