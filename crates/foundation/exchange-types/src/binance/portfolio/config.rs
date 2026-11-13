use crate::binance::portfolio::credentials::BinanceCredentialConfig;
use platform_config::EnvVars;
use platform_errors::{BuildError, BuildResult};
use platform_types::UrlScheme;
use std::time::Duration;

const ENABLED: &str = "PORTFOLIO_BINANCE_ENABLED";
const TESTNET: &str = "PORTFOLIO_BINANCE_TESTNET";
const REST_URL: &str = "PORTFOLIO_BINANCE_REST_URL";
const WEBSOCKET_URL: &str = "PORTFOLIO_BINANCE_WEBSOCKET_URL";
const LISTEN_KEY_REFRESH_SECS: &str = "PORTFOLIO_BINANCE_LISTEN_KEY_REFRESH_SECS";
const RECONNECT_BACKOFF_SECS: &str = "PORTFOLIO_BINANCE_RECONNECT_BACKOFF_SECS";
const DEFAULT_REST_URL: &str = "https://api.binance.com/api/v3";
const DEFAULT_TESTNET_REST_URL: &str = "https://testnet.binance.vision/api/v3";
const DEFAULT_WEBSOCKET_URL: &str = "wss://stream.binance.com:9443/ws";
const DEFAULT_TESTNET_WEBSOCKET_URL: &str = "wss://stream.testnet.binance.vision/ws";

#[derive(Debug, Clone)]
pub struct BinancePortfolioConfig {
    rest_url: String,
    websocket_url: String,
    credentials: Vec<BinanceCredentialConfig>,
    listen_key_refresh: Duration,
    reconnect_backoff: Duration,
}

impl BinancePortfolioConfig {
    pub fn new(
        rest_url: impl Into<String>,
        websocket_url: impl Into<String>,
        listen_key_refresh: Duration,
        reconnect_backoff: Duration,
    ) -> BuildResult<Self> {
        let rest_url = rest_url.into();
        let websocket_url = websocket_url.into();
        Self::validate_url(REST_URL, &rest_url, &UrlScheme::HTTP)?;
        Self::validate_url(WEBSOCKET_URL, &websocket_url, &UrlScheme::WEBSOCKET)?;
        Self::validate_duration(LISTEN_KEY_REFRESH_SECS, listen_key_refresh)?;
        Self::validate_duration(RECONNECT_BACKOFF_SECS, reconnect_backoff)?;
        Ok(Self {
            websocket_url: websocket_url.trim_end_matches('/').to_string(),
            rest_url,
            credentials: Vec::new(),
            listen_key_refresh,
            reconnect_backoff,
        })
    }

    pub fn from_vars(vars: &EnvVars) -> BuildResult<Option<Self>> {
        if !vars.optional_parsed::<bool>(ENABLED)?.unwrap_or(false) {
            return Ok(None);
        }
        let testnet = vars.optional_parsed::<bool>(TESTNET)?.unwrap_or(false);
        let rest_url = Self::url(
            vars,
            REST_URL,
            if testnet {
                DEFAULT_TESTNET_REST_URL
            } else {
                DEFAULT_REST_URL
            },
            &UrlScheme::HTTP,
        )?;
        let websocket_url = Self::url(
            vars,
            WEBSOCKET_URL,
            if testnet {
                DEFAULT_TESTNET_WEBSOCKET_URL
            } else {
                DEFAULT_WEBSOCKET_URL
            },
            &UrlScheme::WEBSOCKET,
        )?;
        let credentials = BinanceCredentialConfig::from_vars(vars)?;
        Ok(Some(Self {
            rest_url,
            websocket_url: websocket_url.trim_end_matches('/').to_string(),
            credentials,
            listen_key_refresh: Self::duration(vars, LISTEN_KEY_REFRESH_SECS, 30 * 60)?,
            reconnect_backoff: Self::duration(vars, RECONNECT_BACKOFF_SECS, 5)?,
        }))
    }

    pub fn rest_url(&self) -> &str {
        &self.rest_url
    }

    pub fn websocket_url(&self) -> &str {
        &self.websocket_url
    }

    pub fn credentials(&self) -> &[BinanceCredentialConfig] {
        &self.credentials
    }

    pub fn listen_key_refresh(&self) -> Duration {
        self.listen_key_refresh
    }

    pub fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }

    fn url(
        vars: &EnvVars,
        name: &str,
        default: &str,
        allowed: &[UrlScheme],
    ) -> BuildResult<String> {
        let value = vars.optional(name)?.unwrap_or_else(|| default.to_string());
        Self::validate_url(name, &value, allowed)?;
        Ok(value)
    }

    fn validate_url(name: &str, value: &str, allowed: &[UrlScheme]) -> BuildResult<()> {
        let parsed = url::Url::parse(value)
            .map_err(|error| BuildError::config_invalid_source(name, error))?;
        if !UrlScheme::from_url(&parsed).is_some_and(|scheme| allowed.contains(&scheme)) {
            return Err(BuildError::config_invalid(
                name,
                "has an unsupported URL scheme",
            ));
        }
        Ok(())
    }

    fn duration(vars: &EnvVars, name: &str, default: u64) -> BuildResult<Duration> {
        let value = vars.optional_parsed::<u64>(name)?.unwrap_or(default);
        let duration = Duration::from_secs(value);
        Self::validate_duration(name, duration)?;
        Ok(duration)
    }

    fn validate_duration(name: &str, value: Duration) -> BuildResult<()> {
        if value.is_zero() {
            return Err(BuildError::config_invalid(
                name,
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}
