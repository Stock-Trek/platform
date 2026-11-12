use platform_config::EnvVars;
use platform_errors::BuildResult;
use platform_types::UrlScheme;
use std::net::SocketAddr;

const BIND_ADDRESS: &str = "PORTFOLIO_REGISTRY_READ_BIND_ADDR";
const JWKS_URL: &str = "PORTFOLIO_REGISTRY_JWKS_URL";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadConfig {
    bind_address: SocketAddr,
    jwks_url: String,
}

impl ReadConfig {
    pub fn from_vars(vars: &EnvVars) -> BuildResult<Self> {
        let jwks_url = vars.required_url(JWKS_URL, &UrlScheme::HTTP)?;
        Ok(Self {
            bind_address: vars.required_parsed(BIND_ADDRESS)?,
            jwks_url,
        })
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub fn jwks_url(&self) -> &str {
        &self.jwks_url
    }
}
