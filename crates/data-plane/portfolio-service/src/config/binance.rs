use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_types::url_scheme::UrlScheme;
use secrecy::SecretString;
use serde::Deserialize;
use std::{collections::BTreeSet, fmt, time::Duration};

const ENABLED: &str = "PORTFOLIO_BINANCE_ENABLED";
const TESTNET: &str = "PORTFOLIO_BINANCE_TESTNET";
const REST_URL: &str = "PORTFOLIO_BINANCE_REST_URL";
const WEBSOCKET_URL: &str = "PORTFOLIO_BINANCE_WEBSOCKET_URL";
const CREDENTIALS: &str = "PORTFOLIO_BINANCE_CREDENTIALS";
const LISTEN_KEY_REFRESH_SECS: &str = "PORTFOLIO_BINANCE_LISTEN_KEY_REFRESH_SECS";
const RECONNECT_BACKOFF_SECS: &str = "PORTFOLIO_BINANCE_RECONNECT_BACKOFF_SECS";
const DEFAULT_REST_URL: &str = "https://api.binance.com/api/v3";
const DEFAULT_TESTNET_REST_URL: &str = "https://testnet.binance.vision/api/v3";
const DEFAULT_WEBSOCKET_URL: &str = "wss://stream.binance.com:9443/ws";
const DEFAULT_TESTNET_WEBSOCKET_URL: &str = "wss://stream.testnet.binance.vision/ws";

#[derive(Debug, Clone)]
pub struct BinanceConfig {
    enabled: bool,
    rest_url: String,
    websocket_url: String,
    credentials: Vec<BinanceCredentialConfig>,
    listen_key_refresh: Duration,
    reconnect_backoff: Duration,
}

#[derive(Clone)]
pub struct BinanceCredentialConfig {
    account: String,
    api_key: String,
    secret: SecretString,
}

impl BinanceConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let enabled = vars.optional_parsed::<bool>(ENABLED)?.unwrap_or(false);
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
        let credentials = Self::parse_credentials(vars)?;
        if enabled && credentials.is_empty() {
            return Err(BpError::config_missing(CREDENTIALS));
        }
        Ok(Self {
            enabled,
            rest_url,
            websocket_url,
            credentials,
            listen_key_refresh: Self::duration(vars, LISTEN_KEY_REFRESH_SECS, 30 * 60)?,
            reconnect_backoff: Self::duration(vars, RECONNECT_BACKOFF_SECS, 5)?,
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
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

    fn url(vars: &EnvVars, name: &str, default: &str, allowed: &[UrlScheme]) -> BpResult<String> {
        let value = vars.optional(name)?.unwrap_or_else(|| default.to_string());
        let parsed = reqwest::Url::parse(&value)
            .map_err(|error| BpError::config_invalid_source(name, error))?;
        if !UrlScheme::from_url(&parsed).is_some_and(|scheme| allowed.contains(&scheme)) {
            return Err(BpError::config_invalid(
                name,
                "has an unsupported URL scheme",
            ));
        }
        Ok(value)
    }

    fn duration(vars: &EnvVars, name: &str, default: u64) -> BpResult<Duration> {
        let value = vars.optional_parsed::<u64>(name)?.unwrap_or(default);
        if value == 0 {
            return Err(BpError::config_invalid(name, "must be greater than zero"));
        }
        Ok(Duration::from_secs(value))
    }

    fn parse_credentials(vars: &EnvVars) -> BpResult<Vec<BinanceCredentialConfig>> {
        let Some(payload) = vars.optional(CREDENTIALS)? else {
            return Ok(Vec::new());
        };
        let entries: Vec<CredentialEntry> = serde_json::from_str(&payload)
            .map_err(|error| BpError::config_invalid_source(CREDENTIALS, error))?;
        let mut seen = BTreeSet::new();
        let mut credentials = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.account.trim().is_empty() {
                return Err(BpError::config_invalid(CREDENTIALS, "account is empty"));
            }
            if entry.api_key.trim().is_empty() {
                return Err(BpError::config_invalid(CREDENTIALS, "api_key is empty"));
            }
            if entry.secret.trim().is_empty() {
                return Err(BpError::config_invalid(CREDENTIALS, "secret is empty"));
            }
            if !seen.insert(entry.account.clone()) {
                return Err(BpError::config_invalid(
                    CREDENTIALS,
                    format!("duplicate account {}", entry.account),
                ));
            }
            credentials.push(BinanceCredentialConfig {
                account: entry.account,
                api_key: entry.api_key,
                secret: SecretString::from(entry.secret),
            });
        }
        Ok(credentials)
    }
}

impl BinanceCredentialConfig {
    pub fn new(account: impl Into<String>, api_key: impl Into<String>, secret: &str) -> Self {
        Self {
            account: account.into(),
            api_key: api_key.into(),
            secret: SecretString::from(secret.to_string()),
        }
    }

    pub fn account(&self) -> &str {
        &self.account
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn secret(&self) -> &SecretString {
        &self.secret
    }
}

impl fmt::Debug for BinanceCredentialConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceCredentialConfig")
            .field("account", &self.account)
            .field("api_key", &self.api_key)
            .field("secret", &"[redacted]")
            .finish()
    }
}

#[derive(Deserialize)]
struct CredentialEntry {
    account: String,
    api_key: String,
    secret: String,
}
