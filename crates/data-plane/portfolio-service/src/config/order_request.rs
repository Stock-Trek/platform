use platform_config::env_vars::EnvVars;
use platform_errors::BuildResult;
use platform_types::url_scheme::UrlScheme;
use std::net::SocketAddr;

const BIND_ADDRESS: &str = "PORTFOLIO_ORDER_REQUEST_BIND_ADDR";
const DELIVERY_URL: &str = "PORTFOLIO_ORDER_REQUEST_URL";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequestConfig {
    bind_address: SocketAddr,
    delivery_url: String,
}

impl OrderRequestConfig {
    pub fn from_vars(vars: &EnvVars) -> BuildResult<Self> {
        let bind_address = vars.required_parsed(BIND_ADDRESS)?;
        let delivery_url = vars.required_url(DELIVERY_URL, &UrlScheme::HTTP)?;
        Ok(Self {
            bind_address,
            delivery_url,
        })
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub fn delivery_url(&self) -> &str {
        &self.delivery_url
    }
}
