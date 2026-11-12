use super::fixtures_tests::Fixtures;
use crate::config::ReplayStartPolicy;
use crate::restore::catalog::{ArchiveObject, ArchiveRecoveryConfig};
use crate::shard::ShardId;
use platform_archive::{ArchiveKind, ArchivePath, ICEBERG_CATALOG_URI, ICEBERG_CATALOG_WAREHOUSE};
use platform_config::env_vars::EnvVars;
use platform_errors::{BuildError, ConfigError};
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

#[test]
fn archive_recovery_selects_only_owned_event_objects() {
    let prefix = "market-data/raw";
    let shard_count = 4;
    let shard = ShardId::new(2);
    let owned = (0..platform_archive::ROUTING_BUCKET_COUNT)
        .map(|index| platform_archive::RoutingBucket::from_index(index).unwrap())
        .find(|bucket| bucket.is_owned_by(shard.get(), shard_count))
        .unwrap();
    let other = (0..platform_archive::ROUTING_BUCKET_COUNT)
        .map(|index| platform_archive::RoutingBucket::from_index(index).unwrap())
        .find(|bucket| !bucket.is_owned_by(shard.get(), shard_count))
        .unwrap();

    let owned_event = ArchivePath::raw_key(
        prefix,
        ArchiveKind::Events,
        TimestampMilliseconds(0),
        owned,
        "owned.parquet",
    )
    .unwrap();
    let other_event = ArchivePath::raw_key(
        prefix,
        ArchiveKind::Events,
        TimestampMilliseconds(0),
        other,
        "other.parquet",
    )
    .unwrap();
    let owned_candle = ArchivePath::raw_key(
        prefix,
        ArchiveKind::Candles,
        TimestampMilliseconds(0),
        owned,
        "candles.parquet",
    )
    .unwrap();
    let owned_compacted = ArchivePath::compacted_key(
        "market-data/compacted",
        &platform_archive::ArchivePartition::Events {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
        },
        TimestampMilliseconds(3_600_000),
        owned,
        "compacted.parquet",
    )
    .unwrap();
    let objects = vec![
        Fixtures::archive_object(&other_event),
        Fixtures::archive_object(&owned_candle),
        Fixtures::archive_object(&owned_compacted),
        Fixtures::archive_object(&owned_event),
    ];

    let mut selected = ArchiveObject::select_for_shard(objects, shard, shard_count);
    ArchiveObject::sort(&mut selected);

    assert_eq!(
        selected,
        vec![
            Fixtures::archive_object(&owned_event),
            Fixtures::archive_object(&owned_compacted)
        ]
    );
}

#[test]
fn archive_recovery_orders_files_by_coverage_hour_not_layer_prefix() {
    let bucket = platform_archive::RoutingBucket::from_index(0).unwrap();
    let older_raw = ArchivePath::raw_key(
        "market-data/raw",
        ArchiveKind::Events,
        TimestampMilliseconds(0),
        bucket,
        "older.parquet",
    )
    .unwrap();
    let newer_compacted = ArchivePath::compacted_key(
        "market-data/compacted",
        &platform_archive::ArchivePartition::Events {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
        },
        TimestampMilliseconds(3_600_000),
        bucket,
        "newer.parquet",
    )
    .unwrap();

    let mut objects = vec![
        Fixtures::archive_object(&newer_compacted),
        Fixtures::archive_object(&older_raw),
    ];
    ArchiveObject::sort(&mut objects);

    assert_eq!(
        objects,
        vec![
            Fixtures::archive_object(&older_raw),
            Fixtures::archive_object(&newer_compacted)
        ]
    );
}

#[test]
fn archive_recovery_ignores_event_objects_without_a_routing_bucket() {
    let key = "market-data/raw/events/year=2024/month=01/day=02/hour=03/legacy.parquet";

    let selected =
        ArchiveObject::select_for_shard(vec![Fixtures::archive_object(key)], ShardId::new(0), 1);

    assert!(selected.is_empty());
}

#[test]
fn archive_recovery_config_is_required_for_policies_that_replay_the_archive() {
    let vars = EnvVars::from_map(Vec::<(String, String)>::new());

    for policy in [ReplayStartPolicy::Checkpoint, ReplayStartPolicy::Archive] {
        assert!(matches!(
            ArchiveRecoveryConfig::from_vars_for_policy(&vars, policy).unwrap_err(),
            BuildError::Config(ConfigError::Missing { name })
                if name == ICEBERG_CATALOG_URI
        ));
    }
}

#[test]
fn archive_recovery_config_is_optional_for_explicit_empty_starts() {
    let vars = EnvVars::from_map(Vec::<(String, String)>::new());

    for policy in [ReplayStartPolicy::Earliest, ReplayStartPolicy::Latest] {
        assert_eq!(
            ArchiveRecoveryConfig::from_vars_for_policy(&vars, policy).unwrap(),
            None
        );
    }
}

#[test]
fn optional_archive_recovery_config_validates_a_configured_catalog() {
    let vars = EnvVars::from_map([(
        ICEBERG_CATALOG_URI.to_string(),
        "http://iceberg-catalog".to_string(),
    )]);

    assert!(matches!(
        ArchiveRecoveryConfig::optional_from_vars(&vars).unwrap_err(),
        BuildError::Config(ConfigError::Missing { name })
            if name == ICEBERG_CATALOG_WAREHOUSE
    ));
}
