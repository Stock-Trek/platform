use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use std::{fmt, net::SocketAddr};

const BIND_ADDRESS: &str = "PORTFOLIO_REGISTRY_ADMIN_BIND_ADDR";
const TOKEN: &str = "PORTFOLIO_REGISTRY_ADMIN_TOKEN";

#[derive(Clone)]
pub struct AdminConfig {
    bind_address: SocketAddr,
    token: SecretString,
}

impl AdminConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let token = vars.required(TOKEN)?;
        if token.len() < 32 {
            return Err(BpError::config_invalid(
                TOKEN,
                "must be at least 32 characters",
            ));
        }
        Ok(Self {
            bind_address: vars.required_parsed(BIND_ADDRESS)?,
            token: SecretString::from(token),
        })
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub fn token(&self) -> &str {
        self.token.expose_secret()
    }
}

impl fmt::Debug for AdminConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminConfig")
            .field("bind_address", &self.bind_address)
            .field("token", &"[REDACTED]")
            .finish()
    }
}
