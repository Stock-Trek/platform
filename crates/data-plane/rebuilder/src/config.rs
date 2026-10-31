use platform_archive::{ArchiveTableLayer, IcebergConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_runtime::Config;
use platform_types::new_types::DurationMilliseconds;
use std::collections::BTreeMap;

const REBUILDER_S3_BUCKET: &str = "REBUILDER_S3_BUCKET";
const REBUILDER_OUTPUT_PREFIX: &str = "REBUILDER_OUTPUT_PREFIX";
const REBUILDER_INTERVALS: &str = "REBUILDER_INTERVALS";
const REBUILDER_ALLOWED_LATENESS_MS: &str = "REBUILDER_ALLOWED_LATENESS_MS";
const REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL: &str = "REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL";
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
    pub max_closed_candles_per_interval: BTreeMap<CandleInterval, usize>,
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
        let max_closed_candles_per_interval =
            Self::max_closed_candles_per_interval(vars, &intervals)?;
        let correction_cutoff_ms: u64 = vars.required_parsed(REBUILDER_CORRECTION_CUTOFF_MS)?;
        if correction_cutoff_ms == 0 {
            return Err(BpError::config_invalid(
                REBUILDER_CORRECTION_CUTOFF_MS,
                "must be greater than zero",
            ));
        }
        if correction_cutoff_ms < u64::try_from(allowed_lateness.0.max(0)).unwrap_or(0) {
            return Err(BpError::config_invalid(
                REBUILDER_CORRECTION_CUTOFF_MS,
                format!("must be at least {REBUILDER_ALLOWED_LATENESS_MS}"),
            ));
        }
        let correction_cutoff =
            DurationMilliseconds(i64::try_from(correction_cutoff_ms).map_err(|error| {
                BpError::config_invalid_source(REBUILDER_CORRECTION_CUTOFF_MS, error)
            })?);
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
            max_closed_candles_per_interval,
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

    fn max_closed_candles_per_interval(
        vars: &EnvVars,
        intervals: &[CandleInterval],
    ) -> BpResult<BTreeMap<CandleInterval, usize>> {
        let value = vars.required(REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL)?;
        let mut caps = BTreeMap::new();
        for entry in value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let Some((label, count)) = entry.split_once('=') else {
                return Err(BpError::config_invalid(
                    REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL,
                    format!("entry {entry:?} must use <interval>=<count>"),
                ));
            };
            let interval = CandleInterval::from_label(label.trim()).map_err(|error| {
                BpError::config_invalid_source(REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL, error)
            })?;
            if !intervals.contains(&interval) {
                return Err(BpError::config_invalid(
                    REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL,
                    format!("interval {interval} is not listed in {REBUILDER_INTERVALS}"),
                ));
            }
            let count: usize = count.trim().parse().map_err(|error| {
                BpError::config_invalid(
                    REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL,
                    format!("invalid candle count for {interval}: {error}"),
                )
            })?;
            if count == 0 {
                return Err(BpError::config_invalid(
                    REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL,
                    format!("interval {interval} must have a positive candle count"),
                ));
            }
            if caps.insert(interval, count).is_some() {
                return Err(BpError::config_invalid(
                    REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL,
                    format!("interval {interval} is listed more than once"),
                ));
            }
        }
        if let Some(missing) = intervals
            .iter()
            .find(|interval| !caps.contains_key(interval))
        {
            return Err(BpError::config_invalid(
                REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL,
                format!("missing a candle count for interval {missing}"),
            ));
        }
        Ok(caps)
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

    fn vars(caps: &str, cutoff: &str) -> EnvVars {
        EnvVars::from_map([
            ("RUST_LOG", "info".to_string()),
            ("REBUILDER_S3_BUCKET", "bucket".to_string()),
            ("REBUILDER_OUTPUT_PREFIX", "output".to_string()),
            ("REBUILDER_INTERVALS", "1m,5m".to_string()),
            ("REBUILDER_ALLOWED_LATENESS_MS", "5000".to_string()),
            (
                "REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL",
                caps.to_string(),
            ),
            ("REBUILDER_CORRECTION_CUTOFF_MS", cutoff.to_string()),
            ("REBUILDER_SOURCE_LAYER", "auto".to_string()),
            ("REBUILDER_OUTPUT_LAYER", "raw".to_string()),
            ("REBUILDER_STATE_EPOCH", "7".to_string()),
            ("REBUILDER_FINISH_OPEN_WINDOWS", "true".to_string()),
            ("REBUILDER_BUCKET_MILLIS", "3600000".to_string()),
            ("REBUILDER_DRY_RUN", "false".to_string()),
            ("ICEBERG_CATALOG_URI", "http://localhost:8181".to_string()),
            ("ICEBERG_CATALOG_WAREHOUSE", "s3://warehouse".to_string()),
            ("ICEBERG_NAMESPACE", "market".to_string()),
        ])
    }

    #[test]
    fn parses_per_interval_caps_and_cutoff() {
        let config = RebuildConfig::from_vars(&vars("1m=1440,5m=288", "60000")).unwrap();

        assert_eq!(
            config.max_closed_candles_per_interval[&CandleInterval::from_label("1m").unwrap()],
            1440
        );
        assert_eq!(
            config.max_closed_candles_per_interval[&CandleInterval::from_label("5m").unwrap()],
            288
        );
        assert_eq!(config.correction_cutoff, DurationMilliseconds(60_000));
    }

    #[test]
    fn rejects_invalid_retention_configuration() {
        for caps in ["1m=1", "1m=0,5m=1", "1m=1,1m=2,5m=1", "1m=1,15m=2"] {
            assert!(
                RebuildConfig::from_vars(&vars(caps, "60000")).is_err(),
                "caps {caps} should be rejected"
            );
        }
        for cutoff in ["0", "4999"] {
            assert!(
                RebuildConfig::from_vars(&vars("1m=1,5m=1", cutoff)).is_err(),
                "cutoff {cutoff} should be rejected"
            );
        }
    }
}
