use platform_errors::{BpError, BpResult};
use platform_types::protocol::Protocol;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BinancePortfolioConfig {
    rest_url: String,
    websocket_url: String,
    listen_key_refresh: Duration,
    reconnect_backoff: Duration,
}

impl BinancePortfolioConfig {
    pub fn new(
        rest_url: impl Into<String>,
        websocket_url: impl Into<String>,
        listen_key_refresh: Duration,
        reconnect_backoff: Duration,
    ) -> BpResult<Self> {
        let rest_url = rest_url.into();
        let websocket_url = websocket_url.into();
        Protocol::Http
            .validate_url("binance rest url", &rest_url)
            .map_err(BpError::external)?;
        Protocol::Websocket
            .validate_url("binance websocket url", &websocket_url)
            .map_err(BpError::external)?;
        if listen_key_refresh.is_zero() {
            return Err(BpError::bad_value(
                "binance listen key refresh",
                listen_key_refresh.as_secs().to_string(),
                "must be greater than zero",
            ));
        }
        if reconnect_backoff.is_zero() {
            return Err(BpError::bad_value(
                "binance reconnect backoff",
                reconnect_backoff.as_secs().to_string(),
                "must be greater than zero",
            ));
        }
        Ok(Self {
            websocket_url: websocket_url.trim_end_matches('/').to_string(),
            rest_url,
            listen_key_refresh,
            reconnect_backoff,
        })
    }

    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    pub fn websocket_url(&self) -> &str {
        &self.websocket_url
    }

    pub fn listen_key_refresh(&self) -> Duration {
        self.listen_key_refresh
    }

    pub fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }
}
