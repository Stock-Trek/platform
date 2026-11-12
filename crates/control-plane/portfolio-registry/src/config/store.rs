use platform_config::EnvVars;
use platform_errors::BuildResult;

const TABLE: &str = "PORTFOLIO_REGISTRY_TABLE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryStoreConfig {
    table: String,
}

impl RegistryStoreConfig {
    pub fn from_vars(vars: &EnvVars) -> BuildResult<Self> {
        Ok(Self {
            table: vars.required(TABLE)?,
        })
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}
