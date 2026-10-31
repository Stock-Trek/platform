use platform_archive::{ArchiveTableLayer, IcebergConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_runtime::Config;
use platform_types::new_types::DurationMilliseconds;

const REBUILDER_S3_BUCKET: &str = "REBUILDER_S3_BUCKET";
const REBUILDER_OUTPUT_PREFIX: &str = "REBUILDER_OUTPUT_PREFIX";
const REBUILDER_INTERVALS: &str = "REBUILDER_INTERVALS";
const REBUILDER_ALLOWED_LATENESS_MS: &str = "REBUILDER_ALLOWED_LATENESS_MS";
const REBUILDER_MAX_CLOSED_CANDLES_PER_KEY: &str = "REBUILDER_MAX_CLOSED_CANDLES_PER_KEY";
const REBUILDER_CORRECTION_CUTOFF_MS: &str = "REBUILDER_CORRECTION_CUTOFF_MS";
const REBUILDER_SOURCE_LAYER: &str = "REBUILDER_SOURCE_LAYER";
const REBUILDER_OUTPUT_LAYER: &str = "REBUILDER_OUTPUT_LAYER";
const REBUILDER_STATE_EPOCH: &str = "REBUILDER_STATE_EPOCH";
const REBUILDER_FINISH_OPEN_WINDOWS: &str = "REBUILDER_FINISH_OPEN_WINDOWS";
const REBUILDER_BUCKET_MILLIS: &str = "REBUILDER_BUCKET_MILLIS";
const REBUILDER_DRY_RUN: &str = "REBUILDER_DRY_RUN";

/// Which Iceberg table the rebuilder reads events from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildSourceLayer {
    /// Read compacted event files plus the raw tail not represented by the
    /// compacted snapshot.
    Auto,
    Raw,
    Compacted,
}

impl RebuildSourceLayer {
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Raw => "raw",
            Self::Compacted => "compacted",
        }
    }

    fn parse(value: &str) -> BpResult<Self> {
        match value {
            "auto" => Ok(Self::Auto),
            "raw" => Ok(Self::Raw),
            "compacted" => Ok(Self::Compacted),
            other => Err(BpError::config_invalid(
                REBUILDER_SOURCE_LAYER,
                format!("must be auto, raw, or compacted, got {other}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RebuildConfig {
    pub log_filter: String,
    pub s3_bucket: String,
    pub output_prefix: String,
    pub intervals: Vec<CandleInterval>,
    pub allowed_lateness: DurationMilliseconds,
    pub max_closed_candles_per_key: usize,
    pub correction_cutoff: DurationMilliseconds,
    pub source_layer: RebuildSourceLayer,
    pub output_layer: ArchiveTableLayer,
    pub state_epoch: u64,
    pub finish_open_windows: bool,
    pub bucket_millis: i64,
    pub dry_run: bool,
    pub iceberg: IcebergConfig,
}

impl RebuildConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let s3_bucket = vars.required(REBUILDER_S3_BUCKET)?;
        let output_prefix = vars.required(REBUILDER_OUTPUT_PREFIX)?;
        let intervals = Self::intervals(vars)?;
        let allowed_lateness =
            DurationMilliseconds(vars.required_parsed(REBUILDER_ALLOWED_LATENESS_MS)?);
        let max_closed_candles_per_key =
            Config::required_positive_usize(vars, REBUILDER_MAX_CLOSED_CANDLES_PER_KEY)?;
        let correction_cutoff_ms = vars
            .required_nonzero_u64(REBUILDER_CORRECTION_CUTOFF_MS)?
            .get();
        let correction_cutoff =
            DurationMilliseconds(i64::try_from(correction_cutoff_ms).map_err(|error| {
                BpError::config_invalid_source(REBUILDER_CORRECTION_CUTOFF_MS, error)
            })?);
        if correction_cutoff < allowed_lateness {
            return Err(BpError::config_invalid(
                REBUILDER_CORRECTION_CUTOFF_MS,
                "must not be shorter than the allowed-lateness window",
            ));
        }
        let source_layer = RebuildSourceLayer::parse(&vars.required(REBUILDER_SOURCE_LAYER)?)?;
        let output_layer = Self::output_layer(vars)?;
        let state_epoch = vars.required_parsed(REBUILDER_STATE_EPOCH)?;
        let finish_open_windows = vars.required_parsed(REBUILDER_FINISH_OPEN_WINDOWS)?;
        let bucket_millis = i64::try_from(Config::required_positive_usize(
            vars,
            REBUILDER_BUCKET_MILLIS,
        )?)
        .map_err(|error| BpError::config_invalid_source(REBUILDER_BUCKET_MILLIS, error))?;
        let dry_run = vars.required_parsed(REBUILDER_DRY_RUN)?;
        let iceberg = IcebergConfig::from_vars(vars)?;

        Ok(Self {
            log_filter,
            s3_bucket,
            output_prefix,
            intervals,
            allowed_lateness,
            max_closed_candles_per_key,
            correction_cutoff,
            source_layer,
            output_layer,
            state_epoch,
            finish_open_windows,
            bucket_millis,
            dry_run,
            iceberg,
        })
    }

    fn output_layer(vars: &EnvVars) -> BpResult<ArchiveTableLayer> {
        let value = vars.required(REBUILDER_OUTPUT_LAYER)?;
        match value.as_str() {
            "raw" => Ok(ArchiveTableLayer::Raw),
            "compacted" => Ok(ArchiveTableLayer::Compacted),
            other => Err(BpError::config_invalid(
                REBUILDER_OUTPUT_LAYER,
                format!("must be raw or compacted, got {other}"),
            )),
        }
    }

    fn intervals(vars: &EnvVars) -> BpResult<Vec<CandleInterval>> {
        let labels = vars.required(REBUILDER_INTERVALS)?;
        let labels = labels
            .split(',')
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .collect::<Vec<_>>();
        if labels.is_empty() {
            return Err(BpError::config_invalid(
                REBUILDER_INTERVALS,
                "must contain at least one interval",
            ));
        }

        let mut intervals = Vec::with_capacity(labels.len());
        for label in labels {
            let interval = CandleInterval::from_label(label)
                .map_err(|error| BpError::config_invalid_source(REBUILDER_INTERVALS, error))?;
            if !intervals.contains(&interval) {
                intervals.push(interval);
            }
        }
        Ok(intervals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_errors::ConfigError;

    fn valid_vars() -> Vec<(String, String)> {
        [
            ("RUST_LOG", "info"),
            (REBUILDER_S3_BUCKET, "market-data"),
            (REBUILDER_OUTPUT_PREFIX, "rebuilt"),
            (REBUILDER_INTERVALS, "1m,5m"),
            (REBUILDER_ALLOWED_LATENESS_MS, "5000"),
            (REBUILDER_MAX_CLOSED_CANDLES_PER_KEY, "10"),
            (REBUILDER_CORRECTION_CUTOFF_MS, "60000"),
            (REBUILDER_SOURCE_LAYER, "raw"),
            (REBUILDER_OUTPUT_LAYER, "raw"),
            (REBUILDER_STATE_EPOCH, "7"),
            (REBUILDER_FINISH_OPEN_WINDOWS, "false"),
            (REBUILDER_BUCKET_MILLIS, "3600000"),
            (REBUILDER_DRY_RUN, "true"),
            ("ICEBERG_CATALOG_URI", "https://catalog.example.com"),
            ("ICEBERG_CATALOG_WAREHOUSE", "s3://market-data/warehouse"),
            ("ICEBERG_NAMESPACE", "market_data"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
    }

    fn parse(overrides: &[(&str, &str)]) -> BpResult<RebuildConfig> {
        let mut vars = valid_vars();
        for (name, value) in overrides {
            if let Some(entry) = vars.iter_mut().find(|(key, _)| key == name) {
                entry.1 = (*value).to_string();
            } else {
                vars.push(((*name).to_string(), (*value).to_string()));
            }
        }
        RebuildConfig::from_vars(&EnvVars::from_map(vars))
    }

    fn assert_invalid(result: BpResult<RebuildConfig>, name: &str) {
        assert!(matches!(
            result,
            Err(BpError::Config(ConfigError::Invalid {
                name: invalid_name,
                ..
            })) if invalid_name == name
        ));
    }

    #[test]
    fn parses_retention_configuration() {
        let config = parse(&[]).unwrap();

        assert_eq!(config.max_closed_candles_per_key, 10);
        assert_eq!(config.correction_cutoff, DurationMilliseconds(60_000));
        assert_eq!(config.allowed_lateness, DurationMilliseconds(5_000));
    }

    #[test]
    fn rejects_an_empty_closed_candle_cap() {
        assert_invalid(
            parse(&[(REBUILDER_MAX_CLOSED_CANDLES_PER_KEY, "0")]),
            REBUILDER_MAX_CLOSED_CANDLES_PER_KEY,
        );
    }

    #[test]
    fn rejects_a_zero_correction_cutoff() {
        assert_invalid(
            parse(&[(REBUILDER_CORRECTION_CUTOFF_MS, "0")]),
            REBUILDER_CORRECTION_CUTOFF_MS,
        );
    }

    #[test]
    fn rejects_a_correction_cutoff_shorter_than_allowed_lateness() {
        assert_invalid(
            parse(&[(REBUILDER_CORRECTION_CUTOFF_MS, "4999")]),
            REBUILDER_CORRECTION_CUTOFF_MS,
        );
    }

    #[test]
    fn rejects_a_correction_cutoff_that_does_not_fit_in_a_duration() {
        assert_invalid(
            parse(&[(REBUILDER_CORRECTION_CUTOFF_MS, "9223372036854775808")]),
            REBUILDER_CORRECTION_CUTOFF_MS,
        );
    }
}
