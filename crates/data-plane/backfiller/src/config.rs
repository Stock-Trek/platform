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
const BACKFILLER_TRADING_MODE: &str = "BACKFILLER_TRADING_MODE";
const BACKFILLER_EXCHANGE_SIGNING_ALGORITHM: &str = "BACKFILLER_EXCHANGE_SIGNING_ALGORITHM";
const BACKFILLER_SIGNING_ALGORITHM: &str = "BACKFILLER_SIGNING_ALGORITHM";

const TRADING_MODE_VARS: &[&str] = &[BACKFILLER_EXCHANGE_TRADING_MODE, BACKFILLER_TRADING_MODE];
const SIGNING_ALGORITHM_VARS: &[&str] = &[
    BACKFILLER_EXCHANGE_SIGNING_ALGORITHM,
    BACKFILLER_SIGNING_ALGORITHM,
];

/// SSM parameter name holding the exchange API key used for historical trades.
pub const EXCHANGE_API_KEY_SECRET: &str = "BACKFILLER_EXCHANGE_API_KEY";
/// SSM parameter name holding the exchange API secret or private signing key.
pub const EXCHANGE_API_SECRET: &str = "BACKFILLER_EXCHANGE_API_SECRET";
pub const EXCHANGE_PRODUCTION_API_KEY_SECRET: &str = "BACKFILLER_EXCHANGE_PRODUCTION_API_KEY";
pub const EXCHANGE_PRODUCTION_API_SECRET: &str = "BACKFILLER_EXCHANGE_PRODUCTION_API_SECRET";
pub const EXCHANGE_TESTNET_API_KEY_SECRET: &str = "BACKFILLER_EXCHANGE_TESTNET_API_KEY";
pub const EXCHANGE_TESTNET_API_SECRET: &str = "BACKFILLER_EXCHANGE_TESTNET_API_SECRET";
pub const EXCHANGE_REAL_API_KEY_SECRET: &str = "BACKFILLER_EXCHANGE_REAL_API_KEY";
pub const EXCHANGE_REAL_API_SECRET: &str = "BACKFILLER_EXCHANGE_REAL_API_SECRET";
pub const EXCHANGE_PAPER_API_KEY_SECRET: &str = "BACKFILLER_EXCHANGE_PAPER_API_KEY";
pub const EXCHANGE_PAPER_API_SECRET: &str = "BACKFILLER_EXCHANGE_PAPER_API_SECRET";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExchangeSecretNames {
    pub api_key: &'static str,
    pub api_secret: &'static str,
}

const REAL_EXCHANGE_SECRETS: [ExchangeSecretNames; 3] = [
    ExchangeSecretNames {
        api_key: EXCHANGE_PRODUCTION_API_KEY_SECRET,
        api_secret: EXCHANGE_PRODUCTION_API_SECRET,
    },
    ExchangeSecretNames {
        api_key: EXCHANGE_REAL_API_KEY_SECRET,
        api_secret: EXCHANGE_REAL_API_SECRET,
    },
    ExchangeSecretNames {
        api_key: EXCHANGE_API_KEY_SECRET,
        api_secret: EXCHANGE_API_SECRET,
    },
];

const PAPER_EXCHANGE_SECRETS: [ExchangeSecretNames; 3] = [
    ExchangeSecretNames {
        api_key: EXCHANGE_TESTNET_API_KEY_SECRET,
        api_secret: EXCHANGE_TESTNET_API_SECRET,
    },
    ExchangeSecretNames {
        api_key: EXCHANGE_PAPER_API_KEY_SECRET,
        api_secret: EXCHANGE_PAPER_API_SECRET,
    },
    ExchangeSecretNames {
        api_key: EXCHANGE_API_KEY_SECRET,
        api_secret: EXCHANGE_API_SECRET,
    },
];

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
        let (trading_mode_name, trading_mode_value) = Self::required_any(vars, TRADING_MODE_VARS)?;
        let trading_mode = trading_mode_value
            .parse::<TradingMode>()
            .map_err(|error| BpError::config_invalid_source(trading_mode_name, error))?;
        let (signing_algorithm_name, signing_algorithm_value) =
            Self::required_any(vars, SIGNING_ALGORITHM_VARS)?;
        let signing_algorithm = signing_algorithm_value
            .parse::<EncryptionAlgorithm>()
            .map_err(|error| BpError::config_invalid_source(signing_algorithm_name, error))?;
        if signing_algorithm != EncryptionAlgorithm::Ed25519 {
            return Err(BpError::config_invalid(
                signing_algorithm_name,
                format!("only ed25519 is supported, got {}", signing_algorithm_value),
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

    pub fn exchange_secret_names(trading_mode: TradingMode) -> &'static [ExchangeSecretNames] {
        match trading_mode {
            TradingMode::Real => &REAL_EXCHANGE_SECRETS,
            TradingMode::Paper => &PAPER_EXCHANGE_SECRETS,
        }
    }

    fn required_any(
        vars: &EnvVars,
        names: &'static [&'static str],
    ) -> BpResult<(&'static str, String)> {
        for name in names {
            if let Some(value) = vars.optional(name)? {
                return Ok((name, value));
            }
        }
        Err(BpError::config_missing(names[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(overrides: &[(&str, &str)]) -> EnvVars {
        let mut values = vec![
            ("RUST_LOG", "info"),
            ("BACKFILLER_EXCHANGE", "binance"),
            ("BACKFILLER_NATS_URL", "nats://localhost:4222"),
            ("BACKFILLER_GAP_SUBJECT_PREFIX", "market.gaps"),
            ("BACKFILLER_EVENT_SUBJECT_PREFIX", "market.events"),
            (
                "BACKFILLER_EXCHANGE_HTTP_URL",
                "https://api.binance.com/api/v3",
            ),
            ("SECRETS_SSM_PATH", "/platform/backfiller/test"),
            ("SECRETS_SSM_DECRYPT", "true"),
            ("BACKFILLER_EXCHANGE_TRADING_MODE", "real"),
            ("BACKFILLER_EXCHANGE_SIGNING_ALGORITHM", "ed25519"),
        ];
        for (name, value) in overrides {
            if let Some(existing) = values.iter_mut().find(|(key, _)| key == name) {
                existing.1 = value;
            } else {
                values.push((name, value));
            }
        }
        EnvVars::from_map(values)
    }

    #[test]
    fn parses_general_exchange_trading_and_signing_settings() {
        let config = BackfillerConfig::from_vars(&vars(&[
            ("BACKFILLER_EXCHANGE_TRADING_MODE", "paper"),
            ("BACKFILLER_EXCHANGE_SIGNING_ALGORITHM", "Ed25519"),
        ]))
        .unwrap();

        assert_eq!(config.trading_mode, TradingMode::Paper);
        assert_eq!(config.signing_algorithm, EncryptionAlgorithm::Ed25519);
    }

    #[test]
    fn accepts_testnet_as_a_paper_trading_mode() {
        let config =
            BackfillerConfig::from_vars(&vars(&[("BACKFILLER_EXCHANGE_TRADING_MODE", "testnet")]))
                .unwrap();

        assert_eq!(config.trading_mode, TradingMode::Paper);
    }

    #[test]
    fn signing_algorithms_other_than_ed25519_are_rejected() {
        let error = BackfillerConfig::from_vars(&vars(&[(
            "BACKFILLER_EXCHANGE_SIGNING_ALGORITHM",
            "hmac_sha256",
        )]))
        .unwrap_err();

        assert!(error.to_string().contains("only ed25519 is supported"));
    }

    #[test]
    fn invalid_trading_modes_are_rejected() {
        let error =
            BackfillerConfig::from_vars(&vars(&[("BACKFILLER_EXCHANGE_TRADING_MODE", "unknown")]))
                .unwrap_err();

        assert!(error.to_string().contains("Unsupported trading mode"));
    }

    #[test]
    fn production_and_paper_use_separate_secret_parameters() {
        assert_eq!(
            BackfillerConfig::exchange_secret_names(TradingMode::Real)[0].api_key,
            EXCHANGE_PRODUCTION_API_KEY_SECRET
        );
        assert_eq!(
            BackfillerConfig::exchange_secret_names(TradingMode::Paper)[0].api_key,
            EXCHANGE_TESTNET_API_KEY_SECRET
        );
    }
}
