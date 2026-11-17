use platform_archive::{
    ArchiveKind, DEFAULT_MAX_BYTES, DEFAULT_MAX_DATA_FILES, DEFAULT_ORPHAN_GRACE,
    DEFAULT_RAW_RETENTION, DEFAULT_SNAPSHOT_RETENTION, MIN_COMPACTED_SNAPSHOTS, MaintenanceConfig,
    PartitionTarget,
};
use platform_config::EnvVars;
use platform_errors::{PlatformError, PlatformResult};
use platform_runtime::Config;
use std::{collections::BTreeMap, time::Duration};

const MAINTAINER_S3_BUCKET: &str = "MAINTAINER_S3_BUCKET";
const MAINTAINER_OUTPUT_PREFIX: &str = "MAINTAINER_OUTPUT_PREFIX";
const MAINTAINER_NATS_URL: &str = "MAINTAINER_NATS_URL";
const MAINTAINER_LEASE_BUCKET: &str = "MAINTAINER_LEASE_BUCKET";
const MAINTAINER_INTERVAL_SECS: &str = "MAINTAINER_INTERVAL_SECS";
const MAINTAINER_DRY_RUN: &str = "MAINTAINER_DRY_RUN";
const MAINTAINER_S3_LIFECYCLE_ENABLED: &str = "MAINTAINER_S3_LIFECYCLE_ENABLED";
const MAINTAINER_RAW_RETENTION_SECS_EVENTS: &str = "MAINTAINER_RAW_RETENTION_SECS_EVENTS";
const MAINTAINER_RAW_RETENTION_SECS_CANDLES: &str = "MAINTAINER_RAW_RETENTION_SECS_CANDLES";
const MAINTAINER_SNAPSHOT_RETENTION_SECS: &str = "MAINTAINER_SNAPSHOT_RETENTION_SECS";
const MAINTAINER_SNAPSHOTS_TO_KEEP: &str = "MAINTAINER_SNAPSHOTS_TO_KEEP";
const MAINTAINER_ORPHAN_GRACE_SECS: &str = "MAINTAINER_ORPHAN_GRACE_SECS";
const MAINTAINER_MAX_PARTITION_DATA_FILES: &str = "MAINTAINER_MAX_PARTITION_DATA_FILES";
const MAINTAINER_MAX_PARTITION_BYTES: &str = "MAINTAINER_MAX_PARTITION_BYTES";
const MAINTAINER_MAX_METADATA_GROWTH_PERCENT: &str = "MAINTAINER_MAX_METADATA_GROWTH_PERCENT";
const MAINTAINER_MAX_WORK_SECS: &str = "MAINTAINER_MAX_WORK_SECS";

#[derive(Debug, Clone)]
pub struct MaintainerConfig {
    pub log_filter: String,
    pub s3_bucket: String,
    pub output_prefix: String,
    pub nats_url: String,
    pub lease_bucket: String,
    pub interval: Duration,
    pub dry_run: bool,
    pub s3_lifecycle_enabled: bool,
    pub maintenance: MaintenanceConfig,
    pub iceberg: platform_archive::IcebergConfig,
}

impl MaintainerConfig {
    pub fn from_env() -> PlatformResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let s3_bucket = vars.required(MAINTAINER_S3_BUCKET)?;
        let output_prefix = vars.required(MAINTAINER_OUTPUT_PREFIX)?;
        let nats_url = Config::required_nats_url(vars, MAINTAINER_NATS_URL)?;
        let lease_bucket = vars.required(MAINTAINER_LEASE_BUCKET)?;
        let interval = Config::required_positive_duration_secs(vars, MAINTAINER_INTERVAL_SECS)?;
        let dry_run = Self::optional_bool(vars, MAINTAINER_DRY_RUN)?.unwrap_or(false);
        let s3_lifecycle_enabled =
            Self::optional_bool(vars, MAINTAINER_S3_LIFECYCLE_ENABLED)?.unwrap_or(true);
        let max_work = Config::required_positive_duration_secs(vars, MAINTAINER_MAX_WORK_SECS)?;
        let maintenance = MaintenanceConfig {
            raw_retention: Self::raw_retention(vars)?,
            snapshot_retention: Self::optional_duration_secs(
                vars,
                MAINTAINER_SNAPSHOT_RETENTION_SECS,
                DEFAULT_SNAPSHOT_RETENTION,
            )?,
            snapshots_to_keep: Self::optional_usize(
                vars,
                MAINTAINER_SNAPSHOTS_TO_KEEP,
                MIN_COMPACTED_SNAPSHOTS,
            )?,
            orphan_grace: Self::optional_duration_secs(
                vars,
                MAINTAINER_ORPHAN_GRACE_SECS,
                DEFAULT_ORPHAN_GRACE,
            )?,
            longest_lease: max_work,
            compacted_target: Self::partition_target(vars)?,
            ledger_target: Self::partition_target(vars)?,
            max_rewrite_commits_per_partition: 1,
            max_metadata_growth_percent: Self::optional_u8(
                vars,
                MAINTAINER_MAX_METADATA_GROWTH_PERCENT,
                10,
            )?,
        };
        maintenance.validate()?;
        let iceberg = platform_archive::IcebergConfig::from_vars(vars)?;

        Ok(Self {
            log_filter,
            s3_bucket,
            output_prefix,
            nats_url,
            lease_bucket,
            interval,
            dry_run,
            s3_lifecycle_enabled,
            maintenance,
            iceberg,
        })
    }

    fn raw_retention(vars: &EnvVars) -> PlatformResult<BTreeMap<ArchiveKind, Duration>> {
        let mut retention = BTreeMap::new();
        for (kind, name) in [
            (ArchiveKind::Events, MAINTAINER_RAW_RETENTION_SECS_EVENTS),
            (ArchiveKind::Candles, MAINTAINER_RAW_RETENTION_SECS_CANDLES),
        ] {
            retention.insert(
                kind,
                Self::optional_duration_secs(vars, name, DEFAULT_RAW_RETENTION)?,
            );
        }
        Ok(retention)
    }

    fn partition_target(vars: &EnvVars) -> PlatformResult<PartitionTarget> {
        Ok(PartitionTarget {
            max_data_files: Self::optional_usize(
                vars,
                MAINTAINER_MAX_PARTITION_DATA_FILES,
                DEFAULT_MAX_DATA_FILES,
            )?,
            max_bytes: Self::optional_u64(vars, MAINTAINER_MAX_PARTITION_BYTES, DEFAULT_MAX_BYTES)?,
        })
    }

    fn optional_bool(vars: &EnvVars, name: &str) -> PlatformResult<Option<bool>> {
        match vars.optional(name)? {
            Some(value) => value
                .parse::<bool>()
                .map(Some)
                .map_err(|error| PlatformError::config_invalid_source(name, error)),
            None => Ok(None),
        }
    }

    fn optional_usize(vars: &EnvVars, name: &str, default: usize) -> PlatformResult<usize> {
        match vars.optional(name)? {
            Some(value) => value
                .parse::<usize>()
                .map_err(|error| PlatformError::config_invalid_source(name, error)),
            None => Ok(default),
        }
    }

    fn optional_u64(vars: &EnvVars, name: &str, default: u64) -> PlatformResult<u64> {
        match vars.optional(name)? {
            Some(value) => value
                .parse::<u64>()
                .map_err(|error| PlatformError::config_invalid_source(name, error)),
            None => Ok(default),
        }
    }

    fn optional_u8(vars: &EnvVars, name: &str, default: u8) -> PlatformResult<u8> {
        match vars.optional(name)? {
            Some(value) => value
                .parse::<u8>()
                .map_err(|error| PlatformError::config_invalid_source(name, error)),
            None => Ok(default),
        }
    }

    fn optional_duration_secs(
        vars: &EnvVars,
        name: &str,
        default: Duration,
    ) -> PlatformResult<Duration> {
        match vars.optional(name)? {
            Some(value) => {
                let seconds = value
                    .parse::<u64>()
                    .map_err(|error| PlatformError::config_invalid_source(name, error))?;
                if seconds == 0 {
                    return Err(PlatformError::config_invalid(
                        name,
                        "must be greater than zero",
                    ));
                }
                Ok(Duration::from_secs(seconds))
            }
            None => Ok(default),
        }
    }
}
