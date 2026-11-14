use platform_config::EnvVars;
use platform_errors::PlatformResult;

const TABLE: &str = "PORTFOLIO_REGISTRY_TABLE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryStoreConfig {
    table: String,
}

impl RegistryStoreConfig {
    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        Ok(Self {
            table: vars.required(TABLE)?,
        })
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}
