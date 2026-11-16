use crate::config::ArchiverConfig;
use platform_config::EnvVars;
use std::collections::HashMap;

struct Vars;

impl Vars {
    fn base() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("RUST_LOG", "info"),
            ("ARCHIVER_NATS_URL", "nats://localhost:4222"),
            ("ARCHIVER_EVENT_SUBJECT_PREFIX", "market.events"),
            ("ARCHIVER_CLOSED_CANDLE_SUBJECT_PREFIX", "market.candles"),
            ("ARCHIVER_S3_BUCKET", "archive"),
            ("ARCHIVER_S3_PREFIX", "raw"),
            ("ARCHIVER_FLUSH_INTERVAL_SECS", "5"),
            ("ARCHIVER_BUCKET_SECS", "3600"),
            ("ARCHIVER_MAX_RECORDS", "1000"),
            ("ARCHIVER_DEDUPE_CAPACITY", "1000"),
            ("ARCHIVER_LEASE_BUCKET", "leases"),
            ("ARCHIVER_LEASE_TTL_SECS", "30"),
            ("ARCHIVER_MAX_WORK_SECS", "20"),
            ("ARCHIVER_RECOVERY_PAGE_FILES", "1000"),
            ("ARCHIVER_RECOVERY_PAGE_BYTES", "67108864"),
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
fn lease_work_must_fit_inside_the_ttl() {
    assert!(ArchiverConfig::from_vars(&Vars::env(&[])).is_ok());
    assert!(ArchiverConfig::from_vars(&Vars::env(&[("ARCHIVER_MAX_WORK_SECS", "30")])).is_err());
}

#[test]
fn lease_tokens_must_be_positive() {
    for token in ["0", "-1"] {
        assert!(
            ArchiverConfig::from_vars(&Vars::env(&[("ARCHIVER_LEASE_INITIAL_TOKEN", token)]))
                .is_err()
        );
    }
}

#[test]
fn lease_bucket_and_ttl_must_be_explicit() {
    assert!(ArchiverConfig::from_vars(&Vars::env(&[("ARCHIVER_LEASE_BUCKET", "")])).is_err());
    assert!(ArchiverConfig::from_vars(&Vars::env(&[("ARCHIVER_LEASE_TTL_SECS", "1")])).is_err());
    assert!(
        ArchiverConfig::from_vars(&Vars::env(&[
            ("ARCHIVER_LEASE_TTL_SECS", "2"),
            ("ARCHIVER_MAX_WORK_SECS", "1"),
        ]))
        .is_ok()
    );
}

#[test]
fn clock_skew_and_recovery_pages_must_be_bounded() {
    assert!(
        ArchiverConfig::from_vars(&Vars::env(&[("ARCHIVER_LEASE_CLOCK_SKEW_SECS", "30")])).is_err()
    );
    assert!(
        ArchiverConfig::from_vars(&Vars::env(&[("ARCHIVER_RECOVERY_PAGE_FILES", "0")])).is_err()
    );
    assert!(
        ArchiverConfig::from_vars(&Vars::env(&[("ARCHIVER_RECOVERY_PAGE_BYTES", "0")])).is_err()
    );
    assert!(
        ArchiverConfig::from_vars(&Vars::env(&[
            ("ARCHIVER_LEASE_CLOCK_SKEW_SECS", "1"),
            ("ARCHIVER_RECOVERY_PAGE_FILES", "100"),
            ("ARCHIVER_RECOVERY_PAGE_BYTES", "1048576"),
        ]))
        .is_ok()
    );
}
