use crate::s3::Bucket;
use chrono::{DateTime, Utc};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use std::time::Duration;

const AWS_LAMBDA_FUNCTION_NAME: &str = "AWS_LAMBDA_FUNCTION_NAME";
const BACKFILL_START: &str = "BACKFILL_START";
const BINANCE_HTTP_PAPER: &str = "BINANCE_HTTP_PAPER";
const BINANCE_HTTP_REAL: &str = "BINANCE_HTTP_REAL";
const DEADLINE_BUFFER_SECS: &str = "DEADLINE_BUFFER_SECS";
const RUST_LOG: &str = "RUST_LOG";
const S3_BUCKET: &str = "S3_BUCKET";

/// Configuration required by the market data process.
#[derive(Clone)]
pub struct EnvConfig {
    pub log_filter: String,
    pub s3_bucket: Bucket,
    pub lambda_function_name: String,
    pub backfill_start: DateTime<Utc>,
    pub deadline_buffer: Duration,
    pub binance_http_paper: String,
    pub binance_http_real: String,
}

impl EnvConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            log_filter: vars.required_log_filter(RUST_LOG)?,
            s3_bucket: Bucket::new(vars.required(S3_BUCKET)?),
            lambda_function_name: vars.required(AWS_LAMBDA_FUNCTION_NAME)?,
            backfill_start: vars.required_timestamp(BACKFILL_START)?,
            deadline_buffer: vars.required_duration_secs(DEADLINE_BUFFER_SECS)?,
            binance_http_paper: vars.required_url(BINANCE_HTTP_PAPER, &["http", "https"])?,
            binance_http_real: vars.required_url(BINANCE_HTTP_REAL, &["http", "https"])?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REQUIRED: [(&str, &str); 7] = [
        (RUST_LOG, "info"),
        (S3_BUCKET, "market-data"),
        (AWS_LAMBDA_FUNCTION_NAME, "platform-market-data"),
        (BACKFILL_START, "2024-01-02"),
        (DEADLINE_BUFFER_SECS, "5"),
        (BINANCE_HTTP_PAPER, "https://testnet.binance.vision/api/v3"),
        (BINANCE_HTTP_REAL, "https://api.binance.com/api/v3"),
    ];

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    #[test]
    fn parses_all_required_values() {
        let config = EnvConfig::from_vars(&vars(&REQUIRED)).unwrap();

        assert_eq!(config.s3_bucket.as_str(), "market-data");
        assert_eq!(config.lambda_function_name, "platform-market-data");
        assert_eq!(
            config.backfill_start.to_rfc3339(),
            "2024-01-02T00:00:00+00:00"
        );
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
                EnvConfig::from_vars(&vars(&values)).is_err(),
                "{missing} should be required"
            );
        }
    }

    #[test]
    fn rejects_invalid_values() {
        for (name, value) in [
            (BACKFILL_START, "not a date"),
            (BINANCE_HTTP_REAL, "wss://api.binance.com/ws"),
            (RUST_LOG, "not a valid [directive"),
        ] {
            let mut values = REQUIRED.to_vec();
            let index = values
                .iter()
                .position(|(required, _)| *required == name)
                .unwrap();
            values[index] = (name, value);

            assert!(
                EnvConfig::from_vars(&vars(&values)).is_err(),
                "{name}={value} should be rejected"
            );
        }
    }
}
