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
const REBUILDER_SOURCE_LAYER: &str = "REBUILDER_SOURCE_LAYER";
const REBUILDER_OUTPUT_LAYER: &str = "REBUILDER_OUTPUT_LAYER";
const REBUILDER_STATE_EPOCH: &str = "REBUILDER_STATE_EPOCH";
const REBUILDER_FINISH_OPEN_WINDOWS: &str = "REBUILDER_FINISH_OPEN_WINDOWS";
const REBUILDER_BUCKET_MILLIS: &str = "REBUILDER_BUCKET_MILLIS";
const REBUILDER_DRY_RUN: &str = "REBUILDER_DRY_RUN";

const DEFAULT_OUTPUT_PREFIX: &str = "market-data/rebuilt";
const DEFAULT_INTERVALS: &str = "1s,1m,5m";
const DEFAULT_ALLOWED_LATENESS_MS: i64 = 5_000;
const DEFAULT_STATE_EPOCH: u64 = 1;
const DEFAULT_BUCKET_MILLIS: usize = 3_600_000;

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
        let output_prefix = vars
            .optional(REBUILDER_OUTPUT_PREFIX)?
            .unwrap_or_else(|| DEFAULT_OUTPUT_PREFIX.to_string());
        let intervals = Self::intervals(vars)?;
        let allowed_lateness = DurationMilliseconds(
            vars.optional_parsed_or(REBUILDER_ALLOWED_LATENESS_MS, DEFAULT_ALLOWED_LATENESS_MS)?,
        );
        let source_layer = RebuildSourceLayer::parse(
            &vars
                .optional(REBUILDER_SOURCE_LAYER)?
                .unwrap_or_else(|| "auto".to_string()),
        )?;
        let output_layer = Self::output_layer(vars)?;
        let state_epoch = vars.optional_parsed_or(REBUILDER_STATE_EPOCH, DEFAULT_STATE_EPOCH)?;
        let finish_open_windows = vars.optional_parsed_or(REBUILDER_FINISH_OPEN_WINDOWS, true)?;
        let bucket_millis = i64::try_from(Config::optional_positive_usize(
            vars,
            REBUILDER_BUCKET_MILLIS,
            DEFAULT_BUCKET_MILLIS,
        )?)
        .map_err(|error| BpError::config_invalid_source(REBUILDER_BUCKET_MILLIS, error))?;
        let dry_run = vars.optional_parsed_or(REBUILDER_DRY_RUN, false)?;
        let iceberg = IcebergConfig::from_vars(vars, &format!("s3://{s3_bucket}/iceberg"))?;

        Ok(Self {
            log_filter,
            s3_bucket,
            output_prefix,
            intervals,
            allowed_lateness,
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
        let value = vars
            .optional(REBUILDER_OUTPUT_LAYER)?
            .unwrap_or_else(|| "raw".to_string());
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
        let labels = vars.optional(REBUILDER_INTERVALS)?;
        let labels = labels
            .as_deref()
            .unwrap_or(DEFAULT_INTERVALS)
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

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    #[test]
    fn defaults_and_interval_parsing_are_stable() {
        let config = RebuildConfig::from_vars(&vars(&[
            ("RUST_LOG", "info"),
            ("ICEBERG_CATALOG_URI", "http://catalog:8181"),
            (REBUILDER_S3_BUCKET, "archive"),
        ]))
        .unwrap();

        assert_eq!(config.output_prefix, DEFAULT_OUTPUT_PREFIX);
        assert_eq!(config.intervals.len(), 3);
        assert_eq!(config.intervals[0].label(), "1s");
        assert_eq!(config.intervals[2].label(), "5m");
        assert_eq!(config.allowed_lateness, DurationMilliseconds(5_000));
        assert_eq!(config.source_layer, RebuildSourceLayer::Auto);
        assert_eq!(config.output_layer, ArchiveTableLayer::Raw);
        assert_eq!(config.state_epoch, DEFAULT_STATE_EPOCH);
        assert!(config.finish_open_windows);
        assert_eq!(config.iceberg.warehouse, "s3://archive/iceberg".to_string());
    }

    #[test]
    fn explicit_source_layer_and_epoch_are_parsed() {
        let config = RebuildConfig::from_vars(&vars(&[
            ("RUST_LOG", "info"),
            ("ICEBERG_CATALOG_URI", "http://catalog:8181"),
            (REBUILDER_S3_BUCKET, "archive"),
            (REBUILDER_SOURCE_LAYER, "compacted"),
            (REBUILDER_OUTPUT_LAYER, "compacted"),
            (REBUILDER_STATE_EPOCH, "42"),
            (REBUILDER_INTERVALS, "1m, 1m, 15m"),
        ]))
        .unwrap();

        assert_eq!(config.source_layer, RebuildSourceLayer::Compacted);
        assert_eq!(config.output_layer, ArchiveTableLayer::Compacted);
        assert_eq!(config.state_epoch, 42);
        assert_eq!(config.intervals.len(), 2);
        assert_eq!(config.intervals[1].label(), "15m");
    }

    #[test]
    fn invalid_source_layer_is_rejected() {
        assert!(
            RebuildConfig::from_vars(&vars(&[
                ("RUST_LOG", "info"),
                ("ICEBERG_CATALOG_URI", "http://catalog:8181"),
                (REBUILDER_S3_BUCKET, "archive"),
                (REBUILDER_SOURCE_LAYER, "warehouse"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn invalid_output_layer_is_rejected() {
        assert!(
            RebuildConfig::from_vars(&vars(&[
                ("RUST_LOG", "info"),
                ("ICEBERG_CATALOG_URI", "http://catalog:8181"),
                (REBUILDER_S3_BUCKET, "archive"),
                (REBUILDER_OUTPUT_LAYER, "warehouse"),
            ]))
            .is_err()
        );
    }
}
