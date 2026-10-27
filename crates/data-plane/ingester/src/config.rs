use crate::{
    nats::{NatsAddress, NatsSubject},
    source::StreamFormat,
};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use stock_trek_types::cex::cex_id::CexId;

const EXCHANGE: &str = "INGESTER_EXCHANGE";
const WEBSOCKET_URL: &str = "INGESTER_WEBSOCKET_URL";
const WEBSOCKET_API_KEY: &str = "INGESTER_WEBSOCKET_API_KEY";
const STREAM_FORMAT: &str = "INGESTER_STREAM_FORMAT";
const NATS_URL: &str = "INGESTER_NATS_URL";
const NATS_SUBJECT_PREFIX: &str = "INGESTER_NATS_SUBJECT_PREFIX";
const LOG_FILTER: &str = "RUST_LOG";

#[derive(Debug, Clone)]
pub struct IngesterConfig {
    pub log_filter: String,
    pub exchange: CexId,
    pub websocket_url: String,
    pub websocket_api_key: Option<String>,
    pub stream_format: StreamFormat,
    pub nats_url: String,
    pub subject_prefix: String,
}

impl IngesterConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let exchange_value = vars.required(EXCHANGE)?;
        let exchange = CexId::try_from(exchange_value.as_str()).map_err(|_| {
            BpError::config_invalid(EXCHANGE, format!("unsupported exchange {exchange_value}"))
        })?;
        let websocket_url = vars.required_url(WEBSOCKET_URL, &["ws", "wss"])?;
        let websocket_api_key = vars.optional(WEBSOCKET_API_KEY)?;
        let stream_format = StreamFormat::for_exchange(&vars.required(STREAM_FORMAT)?, exchange)
            .map_err(|error| BpError::config_invalid_source(STREAM_FORMAT, error))?;
        let nats_url = vars.required(NATS_URL)?;
        NatsAddress::validate(&nats_url)?;
        let subject_prefix = vars.required(NATS_SUBJECT_PREFIX)?;
        NatsSubject::validate(&subject_prefix)?;

        Ok(Self {
            log_filter: vars.required_log_filter(LOG_FILTER)?,
            exchange,
            websocket_url,
            websocket_api_key,
            stream_format,
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
            (EXCHANGE, "binance"),
            (
                WEBSOCKET_URL,
                "wss://stream-sbe.binance.com:9443/stream?streams=btcusdt@trade",
            ),
            (STREAM_FORMAT, "binary"),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market"),
            (LOG_FILTER, "info"),
        ])
    }

    #[test]
    fn reads_ingester_configuration() {
        let config = IngesterConfig::from_vars(&vars()).unwrap();

        assert_eq!(config.exchange, CexId::Binance);
        assert!(matches!(config.stream_format, StreamFormat::Binary(_)));
        assert_eq!(config.websocket_api_key, None);
        assert_eq!(config.subject_prefix, "market");
        assert_eq!(config.log_filter, "info");
    }

    #[test]
    fn reads_an_optional_api_key() {
        let values = EnvVars::from_map([
            (EXCHANGE, "binance"),
            (
                WEBSOCKET_URL,
                "wss://stream-sbe.binance.com:9443/stream?streams=btcusdt@trade",
            ),
            (WEBSOCKET_API_KEY, "secret"),
            (STREAM_FORMAT, "binary"),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market"),
            (LOG_FILTER, "info"),
        ]);

        let config = IngesterConfig::from_vars(&values).unwrap();

        assert_eq!(config.websocket_api_key.as_deref(), Some("secret"));
    }

    #[test]
    fn rejects_unknown_exchanges() {
        let vars = EnvVars::from_map([
            (EXCHANGE, "coinbase"),
            (WEBSOCKET_URL, "wss://ws-feed.exchange.coinbase.com"),
            (STREAM_FORMAT, "json"),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market"),
            (LOG_FILTER, "info"),
        ]);

        assert!(IngesterConfig::from_vars(&vars).is_err());
    }

    #[test]
    fn rejects_a_non_websocket_url() {
        let vars = EnvVars::from_map([
            (EXCHANGE, "binance"),
            (WEBSOCKET_URL, "https://stream.binance.com:9443/stream"),
            (STREAM_FORMAT, "binary"),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market"),
            (LOG_FILTER, "info"),
        ]);

        assert!(IngesterConfig::from_vars(&vars).is_err());
    }

    #[test]
    fn rejects_an_invalid_nats_subject_prefix() {
        let vars = EnvVars::from_map([
            (EXCHANGE, "binance"),
            (
                WEBSOCKET_URL,
                "wss://stream-sbe.binance.com:9443/stream?streams=btcusdt@trade",
            ),
            (STREAM_FORMAT, "binary"),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market.*"),
            (LOG_FILTER, "info"),
        ]);

        assert!(IngesterConfig::from_vars(&vars).is_err());
    }

    #[test]
    fn rejects_an_unknown_stream_format() {
        let vars = EnvVars::from_map([
            (EXCHANGE, "binance"),
            (
                WEBSOCKET_URL,
                "wss://stream-sbe.binance.com:9443/stream?streams=btcusdt@trade",
            ),
            (STREAM_FORMAT, "protobuf"),
            (NATS_URL, "nats://localhost:4222"),
            (NATS_SUBJECT_PREFIX, "market"),
            (LOG_FILTER, "info"),
        ]);

        assert!(IngesterConfig::from_vars(&vars).is_err());
    }
}
