use platform_config::EnvVars;
use platform_errors::PlatformResult;
use platform_runtime::Config;
use std::net::SocketAddr;

const BIND_ADDRESS: &str = "FRONT_END_API_BIND_ADDR";
const JWKS_URL: &str = "FRONT_END_API_JWKS_URL";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontEndApiConfig {
    log_filter: String,
    bind_address: SocketAddr,
    jwks_url: String,
}

impl FrontEndApiConfig {
    pub fn from_env() -> PlatformResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        Ok(Self {
            log_filter: Config::log_filter(vars)?,
            bind_address: vars.required_parsed(BIND_ADDRESS)?,
            jwks_url: vars.required_url(JWKS_URL, &platform_types::UrlScheme::HTTP)?,
        })
    }

    pub fn log_filter(&self) -> &str {
        &self.log_filter
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub fn jwks_url(&self) -> &str {
        &self.jwks_url
    }
}
