use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_nats::{NatsAddress, NatsSubject};
use stock_trek_types::cex::cex_id::CexId;

const BACKFILLER_EXCHANGE: &str = "BACKFILLER_EXCHANGE";
const BACKFILLER_NATS_URL: &str = "BACKFILLER_NATS_URL";
const BACKFILLER_GAP_SUBJECT_PREFIX: &str = "BACKFILLER_GAP_SUBJECT_PREFIX";
const BACKFILLER_EVENT_SUBJECT_PREFIX: &str = "BACKFILLER_EVENT_SUBJECT_PREFIX";
const BACKFILLER_EXCHANGE_HTTP_URL: &str = "BACKFILLER_EXCHANGE_HTTP_URL";
const BACKFILLER_EXCHANGE_API_KEY: &str = "BACKFILLER_EXCHANGE_API_KEY";
const RUST_LOG: &str = "RUST_LOG";

#[derive(Debug, Clone)]
pub struct BackfillerConfig {
    pub log_filter: String,
    pub exchange: CexId,
    pub nats_url: String,
    pub gap_subject_prefix: String,
    pub event_subject_prefix: String,
    pub exchange_http_url: String,
    pub exchange_api_key: Option<String>,
}

impl BackfillerConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = vars.required_log_filter(RUST_LOG)?;
        let exchange_value = vars.required(BACKFILLER_EXCHANGE)?;
        let exchange = CexId::try_from(exchange_value.as_str()).map_err(|_| {
            BpError::config_invalid(
                BACKFILLER_EXCHANGE,
                format!("unsupported exchange {exchange_value}"),
            )
        })?;
        let nats_url = vars.required(BACKFILLER_NATS_URL)?;
        NatsAddress::validate(&nats_url)?;
        let gap_subject_prefix = vars.required(BACKFILLER_GAP_SUBJECT_PREFIX)?;
        NatsSubject::validate(&gap_subject_prefix)?;
        let event_subject_prefix = vars.required(BACKFILLER_EVENT_SUBJECT_PREFIX)?;
        NatsSubject::validate(&event_subject_prefix)?;
        let exchange_http_url =
            vars.required_url(BACKFILLER_EXCHANGE_HTTP_URL, &["http", "https"])?;
        let exchange_api_key = vars.optional(BACKFILLER_EXCHANGE_API_KEY)?;

        Ok(Self {
            log_filter,
            exchange,
            nats_url,
            gap_subject_prefix,
            event_subject_prefix,
            exchange_http_url,
            exchange_api_key,
        })
    }
}
