use crate::source::StreamFormat;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_runtime::Config;
use std::collections::HashMap;
use stock_trek_types::cex::cex_id::CexId;
use url::Url;

const INGESTER_EXCHANGE: &str = "INGESTER_EXCHANGE";
const INGESTER_WEBSOCKET_URL: &str = "INGESTER_WEBSOCKET_URL";
const INGESTER_WEBSOCKET_API_KEY: &str = "INGESTER_WEBSOCKET_API_KEY";
const INGESTER_STREAM_FORMAT: &str = "INGESTER_STREAM_FORMAT";
const INGESTER_NATS_URL: &str = "INGESTER_NATS_URL";
const INGESTER_NATS_SUBJECT_PREFIX: &str = "INGESTER_NATS_SUBJECT_PREFIX";

#[derive(Debug, Clone)]
pub struct IngesterConfig {
    pub log_filter: String,
    pub exchange: CexId,
    pub websocket_url: String,
    pub websocket_api_key: Option<String>,
    pub stream_format: StreamFormat,
    pub nats_url: String,
    pub nats_subject_prefix: String,
}

impl IngesterConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let exchange = Config::required_exchange(vars, INGESTER_EXCHANGE)?;
        let websocket_url = vars.required_url(INGESTER_WEBSOCKET_URL, &["ws", "wss"])?;
        Self::validate_trade_streams(exchange, &websocket_url)?;
        let websocket_api_key = vars.optional(INGESTER_WEBSOCKET_API_KEY)?;
        let stream_format =
            StreamFormat::for_exchange(&vars.required(INGESTER_STREAM_FORMAT)?, exchange)
                .map_err(|error| BpError::config_invalid_source(INGESTER_STREAM_FORMAT, error))?;
        let nats_url = Config::required_nats_url(vars, INGESTER_NATS_URL)?;
        let nats_subject_prefix =
            Config::required_nats_subject(vars, INGESTER_NATS_SUBJECT_PREFIX)?;

        Ok(Self {
            log_filter,
            exchange,
            websocket_url,
            websocket_api_key,
            stream_format,
            nats_url,
            nats_subject_prefix,
        })
    }

    /// Rejects subscribing to both individual and aggregate trade streams for
    /// the same symbol. They carry the same economic trades with independent
    /// sequence numbers, so consuming both would double-count every trade.
    pub(crate) fn validate_trade_streams(exchange: CexId, websocket_url: &str) -> BpResult<()> {
        if exchange != CexId::Binance {
            return Ok(());
        }

        let url = Url::parse(websocket_url)
            .map_err(|error| BpError::config_invalid_source(INGESTER_WEBSOCKET_URL, error))?;
        let mut trade_streams = HashMap::new();

        for stream in Self::websocket_streams(&url) {
            let Some((symbol, suffix)) = stream.split_once('@') else {
                continue;
            };
            let Some(kind) = TradeStreamKind::from_suffix(suffix) else {
                continue;
            };
            let symbol = symbol.to_ascii_uppercase();
            if trade_streams
                .insert(symbol.clone(), kind)
                .is_some_and(|previous| previous != kind)
            {
                return Err(BpError::config_invalid(
                    INGESTER_WEBSOCKET_URL,
                    format!(
                        "must not subscribe to both @trade and @aggTrade for {symbol}; use one trade stream per symbol"
                    ),
                ));
            }
        }

        Ok(())
    }

    fn websocket_streams(url: &Url) -> Vec<String> {
        let mut streams = Vec::new();

        for (key, value) in url.query_pairs() {
            if key.eq_ignore_ascii_case("streams") {
                streams.extend(
                    value
                        .split('/')
                        .map(str::trim)
                        .filter(|stream| !stream.is_empty())
                        .map(str::to_owned),
                );
            }
        }

        streams.extend(
            url.path_segments()
                .into_iter()
                .flatten()
                .filter(|segment| segment.contains('@'))
                .map(str::to_owned),
        );

        streams
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeStreamKind {
    Individual,
    Aggregate,
}

impl TradeStreamKind {
    fn from_suffix(suffix: &str) -> Option<Self> {
        match suffix.to_ascii_lowercase().as_str() {
            "trade" => Some(Self::Individual),
            "aggtrade" => Some(Self::Aggregate),
            _ => None,
        }
    }
}
