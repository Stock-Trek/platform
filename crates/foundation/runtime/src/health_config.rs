use platform_config::EnvVars;
use platform_errors::{PlatformError, PlatformResult};
use std::net::SocketAddr;

pub const HEALTH_BIND_ADDR: &str = "HEALTH_BIND_ADDR";
pub const HEALTH_BIND_ADDRESS: &str = "HEALTH_BIND_ADDRESS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthConfig {
    address: SocketAddr,
}

impl HealthConfig {
    pub const fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn from_env() -> PlatformResult<Option<Self>> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Option<Self>> {
        Self::first_optional(vars, &[HEALTH_BIND_ADDR, HEALTH_BIND_ADDRESS])
    }

    pub fn from_service_env(service: &str) -> PlatformResult<Option<Self>> {
        Self::from_service_vars(&EnvVars::from_env(), service)
    }

    pub fn from_service_vars(vars: &EnvVars, service: &str) -> PlatformResult<Option<Self>> {
        let name = format!("{service}_HEALTH_BIND_ADDR");
        let alias = format!("{service}_HEALTH_BIND_ADDRESS");
        match Self::first_optional(vars, &[name.as_str(), alias.as_str()])? {
            Some(config) => Ok(Some(config)),
            None => Self::from_vars(vars),
        }
    }

    pub fn required_from_env() -> PlatformResult<Self> {
        Self::required_from_vars(&EnvVars::from_env())
    }

    pub fn required_from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        Self::from_vars(vars)?.ok_or_else(|| PlatformError::config_missing(HEALTH_BIND_ADDR))
    }

    fn first_optional(vars: &EnvVars, names: &[&str]) -> PlatformResult<Option<Self>> {
        for name in names {
            if let Some(value) = vars.optional(name)? {
                return Self::parse(name, &value).map(Some);
            }
        }
        Ok(None)
    }

    fn parse(name: &str, value: &str) -> PlatformResult<Self> {
        let address = value
            .parse::<SocketAddr>()
            .map_err(|error| PlatformError::config_invalid_source(name, error))?;
        Ok(Self::new(address))
    }

    pub(crate) fn lookup(service: Option<&str>) -> PlatformResult<Option<Self>> {
        match service {
            Some(service) => Self::from_service_env(service),
            None => Self::from_env(),
        }
    }
}
