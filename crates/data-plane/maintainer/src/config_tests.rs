use super::config::MaintainerConfig;
use platform_archive::{ArchiveKind, MaintenanceConfig, PartitionTarget};
use platform_config::EnvVars;
use std::collections::HashMap;
use std::time::Duration;

struct Vars;

impl Vars {
    fn base() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("RUST_LOG", "info"),
            ("MAINTAINER_S3_BUCKET", "archive"),
            ("MAINTAINER_OUTPUT_PREFIX", "compacted"),
            ("MAINTAINER_NATS_URL", "nats://localhost:4222"),
            ("MAINTAINER_LEASE_BUCKET", "leases"),
            ("MAINTAINER_INTERVAL_SECS", "3600"),
            ("MAINTAINER_MAX_WORK_SECS", "300"),
            ("ICEBERG_CATALOG_URI", "http://localhost:8181"),
            ("ICEBERG_CATALOG_WAREHOUSE", "s3://archive/warehouse"),
            ("ICEBERG_NAMESPACE", "archive"),
        ])
    }

    fn env(overrides: &[(&'static str, &'static str)]) -> EnvVars {
        let mut vars = Self::base();
        for (name, value) in overrides {
            vars.insert(name, value);
        }
        EnvVars::from_map(vars)
    }
}

#[test]
fn defaults_pass_validation() {
    let config = MaintainerConfig::from_vars(&Vars::env(&[])).unwrap();
    assert_eq!(config.interval, Duration::from_secs(3600));
    assert!(!config.dry_run);
    assert!(config.s3_lifecycle_enabled);
    assert_eq!(
        config
            .maintenance
            .raw_retention_for(ArchiveKind::Events)
            .unwrap(),
        platform_archive::DEFAULT_RAW_RETENTION
    );
    assert_eq!(
        config.maintenance.orphan_grace,
        platform_archive::DEFAULT_ORPHAN_GRACE
    );
    assert_eq!(
        config.maintenance.compacted_target,
        PartitionTarget {
            max_data_files: 100,
            max_bytes: 512 * 1024 * 1024,
        }
    );
}

#[test]
fn overrides_change_maintenance_settings() {
    let config = MaintainerConfig::from_vars(&Vars::env(&[
        ("MAINTAINER_DRY_RUN", "true"),
        ("MAINTAINER_S3_LIFECYCLE_ENABLED", "false"),
        ("MAINTAINER_RAW_RETENTION_SECS_EVENTS", "94608000"),
        ("MAINTAINER_ORPHAN_GRACE_SECS", "1209600"),
        ("MAINTAINER_SNAPSHOTS_TO_KEEP", "20"),
    ]))
    .unwrap();
    assert!(config.dry_run);
    assert!(!config.s3_lifecycle_enabled);
    assert_eq!(
        config
            .maintenance
            .raw_retention_for(ArchiveKind::Events)
            .unwrap(),
        Duration::from_secs(94608000)
    );
    assert_eq!(
        config.maintenance.orphan_grace,
        Duration::from_secs(1209600)
    );
    assert_eq!(config.maintenance.snapshots_to_keep, 20);
}

#[test]
fn retention_below_the_floor_is_rejected() {
    assert!(
        MaintainerConfig::from_vars(&Vars::env(&[(
            "MAINTAINER_RAW_RETENTION_SECS_CANDLES",
            "86400"
        )]))
        .is_err()
    );
    assert!(
        MaintainerConfig::from_vars(&Vars::env(&[("MAINTAINER_ORPHAN_GRACE_SECS", "86400")]))
            .is_err()
    );
    assert!(
        MaintainerConfig::from_vars(&Vars::env(&[(
            "MAINTAINER_SNAPSHOT_RETENTION_SECS",
            "86400"
        )]))
        .is_err()
    );
}

#[test]
fn zero_targets_and_percentages_are_rejected() {
    assert!(
        MaintainerConfig::from_vars(&Vars::env(&[("MAINTAINER_MAX_PARTITION_DATA_FILES", "0")]))
            .is_err()
    );
    assert!(
        MaintainerConfig::from_vars(&Vars::env(&[(
            "MAINTAINER_MAX_METADATA_GROWTH_PERCENT",
            "0"
        )]))
        .is_err()
    );
}

#[test]
fn maintenance_defaults_validate() {
    assert!(MaintenanceConfig::default().validate().is_ok());
}
