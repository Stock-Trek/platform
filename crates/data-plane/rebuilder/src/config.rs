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
