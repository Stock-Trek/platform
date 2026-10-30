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
        let output_prefix = vars.required(REBUILDER_OUTPUT_PREFIX)?;
        let intervals = Self::intervals(vars)?;
        let allowed_lateness =
            DurationMilliseconds(vars.required_parsed(REBUILDER_ALLOWED_LATENESS_MS)?);
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

    const REQUIRED: [(&str, &str); 13] = [
        ("RUST_LOG", "info"),
        ("ICEBERG_CATALOG_URI", "http://catalog:8181"),
        ("ICEBERG_CATALOG_WAREHOUSE", "s3://archive/iceberg"),
        ("ICEBERG_NAMESPACE", "market_data"),
        (REBUILDER_S3_BUCKET, "archive"),
        (REBUILDER_OUTPUT_PREFIX, "market-data/rebuilt"),
        (REBUILDER_INTERVALS, "1s,1m,5m"),
        (REBUILDER_ALLOWED_LATENESS_MS, "5000"),
        (REBUILDER_SOURCE_LAYER, "auto"),
        (REBUILDER_OUTPUT_LAYER, "raw"),
        (REBUILDER_STATE_EPOCH, "1"),
        (REBUILDER_FINISH_OPEN_WINDOWS, "true"),
        (REBUILDER_BUCKET_MILLIS, "3600000"),
    ];

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    fn base_vars() -> Vec<(&'static str, &'static str)> {
        let mut values = REQUIRED.to_vec();
        values.push((REBUILDER_DRY_RUN, "false"));
        values
    }

    #[test]
    fn parses_all_required_values() {
        let config = RebuildConfig::from_vars(&vars(&base_vars())).unwrap();

        assert_eq!(config.output_prefix, "market-data/rebuilt");
        assert_eq!(config.intervals.len(), 3);
        assert_eq!(config.intervals[0].label(), "1s");
        assert_eq!(config.intervals[2].label(), "5m");
        assert_eq!(config.allowed_lateness, DurationMilliseconds(5_000));
        assert_eq!(config.source_layer, RebuildSourceLayer::Auto);
        assert_eq!(config.output_layer, ArchiveTableLayer::Raw);
        assert_eq!(config.state_epoch, 1);
        assert!(config.finish_open_windows);
        assert!(!config.dry_run);
        assert_eq!(config.iceberg.warehouse, "s3://archive/iceberg");
    }

    #[test]
    fn every_value_is_required() {
        for (missing, _) in base_vars() {
            let values = base_vars()
                .into_iter()
                .filter(|(name, _)| *name != missing)
                .collect::<Vec<_>>();

            assert!(
                RebuildConfig::from_vars(&vars(&values)).is_err(),
                "{missing} should be required"
            );
        }
    }

    #[test]
    fn explicit_source_layer_and_epoch_are_parsed() {
        let mut values = base_vars();
        let replacements = [
            (REBUILDER_SOURCE_LAYER, "compacted"),
            (REBUILDER_OUTPUT_LAYER, "compacted"),
            (REBUILDER_STATE_EPOCH, "42"),
            (REBUILDER_INTERVALS, "1m, 1m, 15m"),
        ];
        for (name, value) in replacements {
            let index = values.iter().position(|(key, _)| *key == name).unwrap();
            values[index] = (name, value);
        }
        let config = RebuildConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.source_layer, RebuildSourceLayer::Compacted);
        assert_eq!(config.output_layer, ArchiveTableLayer::Compacted);
        assert_eq!(config.state_epoch, 42);
        assert_eq!(config.intervals.len(), 2);
        assert_eq!(config.intervals[1].label(), "15m");
    }

    #[test]
    fn invalid_source_layer_is_rejected() {
        let mut values = base_vars();
        let index = values
            .iter()
            .position(|(name, _)| *name == REBUILDER_SOURCE_LAYER)
            .unwrap();
        values[index] = (REBUILDER_SOURCE_LAYER, "warehouse");

        assert!(RebuildConfig::from_vars(&vars(&values)).is_err());
    }

    #[test]
    fn invalid_output_layer_is_rejected() {
        let mut values = base_vars();
        let index = values
            .iter()
            .position(|(name, _)| *name == REBUILDER_OUTPUT_LAYER)
            .unwrap();
        values[index] = (REBUILDER_OUTPUT_LAYER, "warehouse");

        assert!(RebuildConfig::from_vars(&vars(&values)).is_err());
    }
}
