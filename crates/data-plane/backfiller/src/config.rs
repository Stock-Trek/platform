use platform_aws::secrets::{SSM_DECRYPT_ENV, SSM_PATH_ENV, SsmConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_exchange_types::{encrypt::EncryptionAlgorithm, urls::TradingMode};
use platform_runtime::Config;
use stock_trek_types::cex::cex_id::CexId;

const BACKFILLER_EXCHANGE: &str = "BACKFILLER_EXCHANGE";
const BACKFILLER_NATS_URL: &str = "BACKFILLER_NATS_URL";
const BACKFILLER_GAP_SUBJECT_PREFIX: &str = "BACKFILLER_GAP_SUBJECT_PREFIX";
const BACKFILLER_EVENT_SUBJECT_PREFIX: &str = "BACKFILLER_EVENT_SUBJECT_PREFIX";
const BACKFILLER_EXCHANGE_HTTP_URL: &str = "BACKFILLER_EXCHANGE_HTTP_URL";
const BACKFILLER_EXCHANGE_TRADING_MODE: &str = "BACKFILLER_EXCHANGE_TRADING_MODE";
const BACKFILLER_EXCHANGE_SIGNING_ALGORITHM: &str = "BACKFILLER_EXCHANGE_SIGNING_ALGORITHM";

const EXCHANGE_SECRET_PREFIX: &str = "BACKFILLER_EXCHANGE";

/// SSM parameter names holding an exchange's API key and secret or private
/// signing key for one trading mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeSecretNames {
    pub api_key: String,
    pub api_secret: String,
}

impl ExchangeSecretNames {
    pub fn new(exchange: CexId, trading_mode: TradingMode) -> Self {
        let exchange = exchange.to_string().to_uppercase();
        let trading_mode = trading_mode.to_string().to_uppercase();
        let prefix = format!("{EXCHANGE_SECRET_PREFIX}_{exchange}_{trading_mode}");
        Self {
            api_key: format!("{prefix}_API_KEY"),
            api_secret: format!("{prefix}_API_SECRET"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BackfillerConfig {
    pub log_filter: String,
    pub exchange: CexId,
    pub trading_mode: TradingMode,
    pub signing_algorithm: EncryptionAlgorithm,
    pub nats_url: String,
    pub gap_subject_prefix: String,
    pub event_subject_prefix: String,
    pub exchange_http_url: String,
    pub ssm: SsmConfig,
}

impl BackfillerConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let exchange = Config::required_exchange(vars, BACKFILLER_EXCHANGE)?;
        let trading_mode = vars.required_parsed::<TradingMode>(BACKFILLER_EXCHANGE_TRADING_MODE)?;
        let signing_algorithm =
            vars.required_parsed::<EncryptionAlgorithm>(BACKFILLER_EXCHANGE_SIGNING_ALGORITHM)?;
        if signing_algorithm != EncryptionAlgorithm::Ed25519 {
            return Err(BpError::config_invalid(
                BACKFILLER_EXCHANGE_SIGNING_ALGORITHM,
                "only ed25519 is supported",
            ));
        }
        let nats_url = Config::required_nats_url(vars, BACKFILLER_NATS_URL)?;
        let gap_subject_prefix =
            Config::required_nats_subject(vars, BACKFILLER_GAP_SUBJECT_PREFIX)?;
        let event_subject_prefix =
            Config::required_nats_subject(vars, BACKFILLER_EVENT_SUBJECT_PREFIX)?;
        let exchange_http_url =
            vars.required_url(BACKFILLER_EXCHANGE_HTTP_URL, &["http", "https"])?;
        let ssm = SsmConfig::new(
            vars.required(SSM_PATH_ENV)?,
            vars.required_parsed(SSM_DECRYPT_ENV)?,
        )?;

        Ok(Self {
            log_filter,
            exchange,
            trading_mode,
            signing_algorithm,
            nats_url,
            gap_subject_prefix,
            event_subject_prefix,
            exchange_http_url,
            ssm,
        })
    }

    pub fn exchange_secret_names(
        exchange: CexId,
        trading_mode: TradingMode,
    ) -> ExchangeSecretNames {
        ExchangeSecretNames::new(exchange, trading_mode)
    }
}
