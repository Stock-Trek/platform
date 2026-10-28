use crate::source::StreamFormat;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_runtime::Config;
use stock_trek_types::cex::cex_id::CexId;

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
        let exchange_value = vars.required(INGESTER_EXCHANGE)?;
        let exchange = CexId::try_from(exchange_value.as_str()).map_err(|_| {
            BpError::config_invalid(
                INGESTER_EXCHANGE,
                format!("unsupported exchange {exchange_value}"),
            )
        })?;
        let websocket_url = vars.required_url(INGESTER_WEBSOCKET_URL, &["ws", "wss"])?;
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
}
