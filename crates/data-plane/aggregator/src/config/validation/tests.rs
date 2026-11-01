use super::super::AggregatorConfig;
use super::implementation::{
    ASSET_ID_COUNT, CEX_ID_COUNT, ESTIMATED_BYTES_PER_CLOSED_CANDLE, RetentionLimits,
};
use platform_market_data::candle::{AlignedWindow, parse_interval};
use std::collections::BTreeMap;
use std::time::Duration;

fn intervals() -> Vec<AlignedWindow> {
    vec![parse_interval("1m").unwrap(), parse_interval("5m").unwrap()]
}

#[test]
fn cap_parsing_requires_one_positive_entry_per_interval() {
    let intervals = intervals();
    assert_eq!(
        AggregatorConfig::parse_max_closed_candles_per_interval(
            "caps",
            "1m=1440,5m=288",
            &intervals,
        )
        .unwrap()
        .len(),
        2
    );

    for value in [
        "1m=0,5m=1",
        "1m=-1,5m=1",
        "1m=1",
        "1m=1,1m=2,5m=1",
        "1m=1,15m=2",
        "1m,5m=1",
    ] {
        assert!(
            AggregatorConfig::parse_max_closed_candles_per_interval("caps", value, &intervals)
                .is_err(),
            "{value} should be rejected"
        );
    }
}

#[test]
fn retention_limits_use_the_full_static_pair_ceiling() {
    let intervals = intervals();
    let caps = BTreeMap::from([(intervals[0], 2), (intervals[1], 3)]);

    let limits = RetentionLimits::calculate(&intervals, &caps, 1).unwrap();

    let static_pair_ceiling = CEX_ID_COUNT * ASSET_ID_COUNT * ASSET_ID_COUNT;
    assert_eq!(limits.static_pair_ceiling, static_pair_ceiling);
    assert_eq!(limits.static_key_ceiling, static_pair_ceiling * 2);
    let worst_case = static_pair_ceiling * 5;
    assert_eq!(limits.worst_case_retained_candles_per_shard, worst_case);
    assert_eq!(
        limits.estimated_memory_bytes,
        worst_case * ESTIMATED_BYTES_PER_CLOSED_CANDLE
    );

    let per_shard_two = RetentionLimits::calculate(&intervals, &caps, 2).unwrap();
    assert_eq!(
        per_shard_two.worst_case_retained_candles_per_shard,
        worst_case.div_ceil(2)
    );
}

#[test]
fn budget_boundary_is_inclusive() {
    let intervals = intervals();
    let caps = BTreeMap::from([(intervals[0], 2), (intervals[1], 3)]);
    let required = RetentionLimits::calculate(&intervals, &caps, 1)
        .unwrap()
        .estimated_memory_bytes as usize;

    assert!(
            AggregatorConfig::validate_retention(
                &intervals,
                &caps,
                1,
                Duration::ZERO,
                1_000,
                required,
            )
            .is_ok()
        );
    assert!(
        AggregatorConfig::validate_retention(
            &intervals,
            &caps,
            1,
            Duration::ZERO,
            1_000,
            required - 1,
        )
        .is_err()
    );
}

#[test]
fn rejection_checks_cutoff_and_overflow() {
    let intervals = intervals();
    let caps = BTreeMap::from([(intervals[0], 1), (intervals[1], 1)]);

    assert!(
        AggregatorConfig::validate_retention(
            &intervals,
            &BTreeMap::from([(intervals[0], 1)]),
            1,
            Duration::ZERO,
            1_000,
            usize::MAX,
        )
        .is_err()
    );
    assert!(
        AggregatorConfig::validate_retention(
            &intervals,
            &BTreeMap::from([(intervals[0], 1), (intervals[1], 0)]),
            1,
            Duration::ZERO,
            1_000,
            usize::MAX,
        )
        .is_err()
    );
    assert!(
        AggregatorConfig::validate_retention(
            &intervals,
            &BTreeMap::from([(intervals[0], 1), (intervals[1], 1)]),
            1,
            Duration::ZERO,
            1_000,
            usize::MAX,
        )
        .is_ok()
    );

    assert!(
        AggregatorConfig::validate_retention(
            &intervals,
            &caps,
            1,
            Duration::from_millis(1_000),
            999,
            1,
        )
        .is_err()
    );
    assert!(
        AggregatorConfig::validate_retention(&intervals, &caps, 1, Duration::ZERO, 0, 1,).is_err()
    );

    let overflow_caps = BTreeMap::from([(intervals[0], usize::MAX), (intervals[1], usize::MAX)]);
    assert!(RetentionLimits::calculate(&intervals, &overflow_caps, 1).is_err());
}
