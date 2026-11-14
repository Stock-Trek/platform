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

    pub fn from_env() -> PlatformResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        Self::required(vars, &[HEALTH_BIND_ADDR, HEALTH_BIND_ADDRESS])
    }

    pub fn from_service_env(service: &str) -> PlatformResult<Self> {
        Self::from_service_vars(&EnvVars::from_env(), service)
    }

    pub fn from_service_vars(vars: &EnvVars, service: &str) -> PlatformResult<Self> {
        let name = format!("{service}_HEALTH_BIND_ADDR");
        let alias = format!("{service}_HEALTH_BIND_ADDRESS");
        match Self::first(vars, &[name.as_str(), alias.as_str()])? {
            Some(config) => Ok(config),
            None => Self::from_vars(vars),
        }
    }

    fn required(vars: &EnvVars, names: &[&str]) -> PlatformResult<Self> {
        Self::first(vars, names)?.ok_or_else(|| {
            PlatformError::config_missing(names.first().copied().unwrap_or(HEALTH_BIND_ADDR))
        })
    }

    fn first(vars: &EnvVars, names: &[&str]) -> PlatformResult<Option<Self>> {
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

    pub(crate) fn lookup(service: Option<&str>) -> PlatformResult<Self> {
        match service {
            Some(service) => Self::from_service_env(service),
            None => Self::from_env(),
        }
    }
}
