use platform_aws::secrets::{SSM_DECRYPT_ENV, SSM_PATH_ENV, SsmConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;
use stock_trek_types::cex::cex_id::CexId;

const BACKFILLER_EXCHANGE: &str = "BACKFILLER_EXCHANGE";
const BACKFILLER_NATS_URL: &str = "BACKFILLER_NATS_URL";
const BACKFILLER_GAP_SUBJECT_PREFIX: &str = "BACKFILLER_GAP_SUBJECT_PREFIX";
const BACKFILLER_EVENT_SUBJECT_PREFIX: &str = "BACKFILLER_EVENT_SUBJECT_PREFIX";
const BACKFILLER_EXCHANGE_HTTP_URL: &str = "BACKFILLER_EXCHANGE_HTTP_URL";

/// SSM parameter name holding the exchange API key used for historical trades.
pub const EXCHANGE_API_KEY_SECRET: &str = "BACKFILLER_EXCHANGE_API_KEY";

#[derive(Debug, Clone)]
pub struct BackfillerConfig {
    pub log_filter: String,
    pub exchange: CexId,
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
            nats_url,
            gap_subject_prefix,
            event_subject_prefix,
            exchange_http_url,
            ssm,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REQUIRED: [(&str, &str); 8] = [
        (platform_runtime::Config::RUST_LOG, "info"),
        (BACKFILLER_EXCHANGE, "binance"),
        (BACKFILLER_NATS_URL, "nats://localhost:4222"),
        (BACKFILLER_GAP_SUBJECT_PREFIX, "market.gaps"),
        (BACKFILLER_EVENT_SUBJECT_PREFIX, "market.events"),
        (BACKFILLER_EXCHANGE_HTTP_URL, "https://api.binance.com"),
        (SSM_PATH_ENV, "/platform/backfiller/production"),
        (SSM_DECRYPT_ENV, "true"),
    ];

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    #[test]
    fn parses_required_values_and_normalises_the_ssm_path() {
        let config = BackfillerConfig::from_vars(&vars(&REQUIRED)).unwrap();

        assert_eq!(config.exchange, CexId::Binance);
        assert_eq!(config.ssm.path, "/platform/backfiller/production");
        assert!(config.ssm.decrypt);
    }

    #[test]
    fn reads_the_ssm_decrypt_flag() {
        let mut values = REQUIRED.to_vec();
        let index = values
            .iter()
            .position(|(name, _)| *name == SSM_DECRYPT_ENV)
            .unwrap();
        values[index] = (SSM_DECRYPT_ENV, "false");

        let config = BackfillerConfig::from_vars(&vars(&values)).unwrap();

        assert!(!config.ssm.decrypt);
    }

    #[test]
    fn every_value_is_required() {
        for (missing, _) in REQUIRED {
            let values = REQUIRED
                .iter()
                .copied()
                .filter(|(name, _)| *name != missing)
                .collect::<Vec<_>>();

            assert!(
                BackfillerConfig::from_vars(&vars(&values)).is_err(),
                "{missing} should be required"
            );
        }
    }

    #[test]
    fn rejects_invalid_values() {
        for (name, value) in [
            (BACKFILLER_EXCHANGE, "kraken"),
            (BACKFILLER_NATS_URL, "http://localhost:4222"),
            (BACKFILLER_GAP_SUBJECT_PREFIX, "market.*"),
            (BACKFILLER_EXCHANGE_HTTP_URL, "ftp://api.binance.com"),
            (SSM_PATH_ENV, "/platform/backfiller/production/"),
            (SSM_DECRYPT_ENV, "not a bool"),
        ] {
            let mut values = REQUIRED.to_vec();
            if let Some(index) = values.iter().position(|(required, _)| *required == name) {
                values[index] = (name, value);
            } else {
                values.push((name, value));
            }

            assert!(
                BackfillerConfig::from_vars(&vars(&values)).is_err(),
                "{name}={value} should be rejected"
            );
        }
    }
}
