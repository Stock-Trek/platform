use crate::config::CompactorConfig;
use platform_config::EnvVars;
use std::collections::HashMap;

struct Vars;

impl Vars {
    fn base() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("RUST_LOG", "info"),
            ("COMPACTOR_S3_BUCKET", "archive"),
            ("COMPACTOR_S3_PREFIX", "raw"),
            ("COMPACTOR_OUTPUT_PREFIX", "compacted"),
            ("COMPACTOR_POLL_INTERVAL_SECS", "5"),
            ("COMPACTOR_SETTLE_SECS", "5"),
            ("COMPACTOR_MIN_SOURCE_FILES", "2"),
            ("COMPACTOR_SPILL_RECORDS", "1000"),
            ("COMPACTOR_NATS_URL", "nats://localhost:4222"),
            ("COMPACTOR_LEASE_BUCKET", "leases"),
            ("COMPACTOR_LEASE_TTL_SECS", "30"),
            ("COMPACTOR_MAX_WORK_SECS", "20"),
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
fn lease_work_must_fit_inside_every_kind_ttl() {
    assert!(CompactorConfig::from_vars(&Vars::env(&[])).is_ok());
    assert!(CompactorConfig::from_vars(&Vars::env(&[("COMPACTOR_MAX_WORK_SECS", "30")])).is_err());
    assert!(
        CompactorConfig::from_vars(&Vars::env(&[("COMPACTOR_LEASE_TTL_SECS_EVENTS", "15"),]))
            .is_err()
    );
}

#[test]
fn lease_tokens_and_ttls_must_be_positive() {
    for token in ["0", "-1"] {
        assert!(
            CompactorConfig::from_vars(&Vars::env(&[("COMPACTOR_LEASE_INITIAL_TOKEN", token)]))
                .is_err()
        );
    }
    assert!(
        CompactorConfig::from_vars(&Vars::env(&[("COMPACTOR_LEASE_TTL_SECS_CANDLES", "0")]))
            .is_err()
    );
}

#[test]
fn every_lease_bucket_must_be_explicit() {
    assert!(CompactorConfig::from_vars(&Vars::env(&[("COMPACTOR_LEASE_BUCKET", "")])).is_err());
}

#[test]
fn lease_ttls_are_at_least_two_seconds() {
    assert!(
        CompactorConfig::from_vars(&Vars::env(&[("COMPACTOR_LEASE_TTL_SECS_EVENTS", "1")]))
            .is_err()
    );
    assert!(
        CompactorConfig::from_vars(&Vars::env(&[
            ("COMPACTOR_LEASE_TTL_SECS", "2"),
            ("COMPACTOR_LEASE_TTL_SECS_EVENTS", "2"),
            ("COMPACTOR_LEASE_TTL_SECS_CANDLES", "2"),
            ("COMPACTOR_MAX_WORK_SECS", "1"),
        ]))
        .is_ok()
    );
}

#[test]
fn clock_skew_must_be_shorter_than_every_ttl() {
    assert!(
        CompactorConfig::from_vars(&Vars::env(&[("COMPACTOR_LEASE_CLOCK_SKEW_SECS", "30")]))
            .is_err()
    );
    assert!(
        CompactorConfig::from_vars(&Vars::env(&[
            ("COMPACTOR_LEASE_CLOCK_SKEW_SECS", "1"),
            ("COMPACTOR_LEASE_TTL_SECS_EVENTS", "4"),
            ("COMPACTOR_LEASE_TTL_SECS_CANDLES", "4"),
            ("COMPACTOR_MAX_WORK_SECS", "1"),
        ]))
        .is_ok()
    );
}
