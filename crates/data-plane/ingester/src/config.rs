use crate::nats::{validate_subject, validate_url};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;

const BINANCE_WEBSOCKET_URL: &str = "INGESTER_BINANCE_WEBSOCKET_URL";
const NATS_URL: &str = "INGESTER_NATS_URL";
const NATS_SUBJECT_PREFIX: &str = "INGESTER_NATS_SUBJECT_PREFIX";
const LOG_FILTER: &str = "RUST_LOG";

#[derive(Debug, Clone)]
pub struct IngesterConfig {
    pub log_filter: String,
    pub binance_websocket_url: String,
    pub nats_url: String,
    pub subject_prefix: String,
}

impl IngesterConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let binance_websocket_url = vars.required_url(BINANCE_WEBSOCKET_URL, &["ws", "wss"])?;
        let nats_url = vars.required(NATS_URL)?;
        validate_url(&nats_url)?;
        let subject_prefix = vars.required(NATS_SUBJECT_PREFIX)?;
        validate_subject(&subject_prefix)?;

        Ok(Self {
            log_filter: vars.required_log_filter(LOG_FILTER)?,
            binance_websocket_url,
            nats_url,
            subject_prefix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> EnvVars {
        EnvVars::from_map([
            (
                BINANCE_WEBSOCKET_URL,
                "wss://stream.binance.com:9443/stream?streams=btcusdt@trade",
            ),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market"),
            (LOG_FILTER, "info"),
        ])
    }

    #[test]
    fn reads_ingester_configuration() {
        let config = IngesterConfig::from_vars(&vars()).unwrap();

        assert_eq!(config.subject_prefix, "market");
        assert_eq!(config.log_filter, "info");
    }

    #[test]
    fn rejects_a_non_websocket_binance_url() {
        let vars = EnvVars::from_map([
            (
                BINANCE_WEBSOCKET_URL,
                "https://stream.binance.com:9443/stream",
            ),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market"),
            (LOG_FILTER, "info"),
        ]);

        assert!(IngesterConfig::from_vars(&vars).is_err());
    }

    #[test]
    fn rejects_an_invalid_nats_subject_prefix() {
        let vars = EnvVars::from_map([
            (
                BINANCE_WEBSOCKET_URL,
                "wss://stream.binance.com:9443/stream?streams=btcusdt@trade",
            ),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market.*"),
            (LOG_FILTER, "info"),
        ]);

        assert!(IngesterConfig::from_vars(&vars).is_err());
    }
}
