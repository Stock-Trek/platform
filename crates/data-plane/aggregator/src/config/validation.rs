use super::env::{
    AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES, AGGREGATOR_CORRECTION_CUTOFF_MS,
    AGGREGATOR_INTERVALS, AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_types::new_types::DurationMilliseconds;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};
use strum::EnumCount;

pub(super) const ESTIMATED_BYTES_PER_CLOSED_CANDLE: u64 = 512;

pub(super) fn validate_kv_bucket(name: &str, bucket: &str) -> BpResult<()> {
    let valid = !bucket.is_empty()
        && bucket
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'));
    if valid {
        Ok(())
    } else {
        Err(BpError::config_invalid(
            name,
            "must only contain alphanumeric characters, dashes, and underscores",
        ))
    }
}

pub(super) fn parse_max_closed_candles_per_interval(
    name: &str,
    value: &str,
    intervals: &[CandleInterval],
) -> BpResult<BTreeMap<CandleInterval, usize>> {
    let mut caps = BTreeMap::new();
    for entry in value.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        let Some((label, count)) = entry.split_once('=') else {
            return Err(BpError::config_invalid(
                name,
                format!("entry {entry:?} must use <interval>=<count>"),
            ));
        };
        let interval = CandleInterval::from_label(label.trim())
            .map_err(|error| BpError::config_invalid_source(name, error))?;
        if !intervals.contains(&interval) {
            return Err(BpError::config_invalid(
                name,
                format!(
                    "interval {} is not listed in {AGGREGATOR_INTERVALS}",
                    interval
                ),
            ));
        }
        let count: usize = count.trim().parse().map_err(|error| {
            BpError::config_invalid(
                name,
                format!("invalid candle count for {}: {error}", interval),
            )
        })?;
        if count == 0 {
            return Err(BpError::config_invalid(
                name,
                format!("interval {} must have a positive candle count", interval),
            ));
        }
        if caps.insert(interval, count).is_some() {
            return Err(BpError::config_invalid(
                name,
                format!("interval {} is listed more than once", interval),
            ));
        }
    }

    let configured = intervals.iter().copied().collect::<BTreeSet<_>>();
    let missing = configured
        .iter()
        .filter(|interval| !caps.contains_key(interval))
        .map(|interval| interval.label())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(BpError::config_invalid(
            name,
            format!(
                "missing a candle count for interval(s): {}",
                missing.join(", ")
            ),
        ));
    }
    let unknown = caps
        .keys()
        .filter(|interval| !configured.contains(interval))
        .map(|interval| interval.label())
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(BpError::config_invalid(
            name,
            format!("unknown interval(s): {}", unknown.join(", ")),
        ));
    }

    Ok(caps)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_retention(
    intervals: &[CandleInterval],
    caps: &BTreeMap<CandleInterval, usize>,
    shard_count: usize,
    allowed_lateness: Duration,
    correction_cutoff_ms: u64,
    memory_budget_bytes: usize,
) -> BpResult<()> {
    let configured = intervals.iter().copied().collect::<BTreeSet<_>>();
    let missing = configured
        .iter()
        .filter(|interval| !caps.contains_key(interval))
        .map(|interval| interval.label())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(BpError::config_invalid(
            AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL,
            format!(
                "missing a candle count for interval(s): {}",
                missing.join(", ")
            ),
        ));
    }
    let unknown = caps
        .keys()
        .filter(|interval| !configured.contains(interval))
        .map(|interval| interval.label())
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(BpError::config_invalid(
            AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL,
            format!("unknown interval(s): {}", unknown.join(", ")),
        ));
    }
    if caps.values().any(|cap| *cap == 0) {
        return Err(BpError::config_invalid(
            AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL,
            "every interval must have a positive candle count",
        ));
    }

    if correction_cutoff_ms == 0 {
        return Err(BpError::config_invalid(
            AGGREGATOR_CORRECTION_CUTOFF_MS,
            "must be greater than zero",
        ));
    }
    let correction_cutoff = Duration::from_millis(correction_cutoff_ms);
    if correction_cutoff < allowed_lateness {
        return Err(BpError::config_invalid(
            AGGREGATOR_CORRECTION_CUTOFF_MS,
            format!("must be at least AGGREGATOR_ALLOWED_LATENESS_MS ({allowed_lateness:?})"),
        ));
    }

    let limits = RetentionLimits::calculate(intervals, caps, shard_count)?;
    let memory_budget_bytes = u64::try_from(memory_budget_bytes).map_err(|error| {
        BpError::config_invalid_source(AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES, error)
    })?;
    if limits.estimated_memory_bytes > memory_budget_bytes {
        return Err(BpError::config_invalid(
            AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES,
            format!(
                "worst-case closed-candle state needs {} bytes for {} retained candles per shard \
                 (static pair ceiling {}, static candle-key ceiling {}), exceeding the {} byte budget; \
                 caps: {}",
                limits.estimated_memory_bytes,
                limits.worst_case_retained_candles_per_shard,
                limits.static_pair_ceiling,
                limits.static_key_ceiling,
                memory_budget_bytes,
                format_caps(caps),
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RetentionLimits {
    pub static_pair_ceiling: u64,
    pub static_key_ceiling: u64,
    pub worst_case_retained_candles_per_shard: u64,
    pub estimated_memory_bytes: u64,
}

impl RetentionLimits {
    pub(super) fn calculate(
        intervals: &[CandleInterval],
        caps: &BTreeMap<CandleInterval, usize>,
        shard_count: usize,
    ) -> BpResult<Self> {
        if shard_count == 0 {
            return Err(BpError::config_invalid(
                super::env::AGGREGATOR_SHARD_COUNT,
                "must be greater than zero",
            ));
        }
        let asset_id_count = u64::try_from(AssetId::COUNT).map_err(|_| overflow_error())?;
        let cex_id_count = u64::try_from(CexId::COUNT).map_err(|_| overflow_error())?;
        let asset_pairs = asset_id_count
            .checked_mul(asset_id_count)
            .ok_or_else(overflow_error)?;
        let static_pair_ceiling = cex_id_count
            .checked_mul(asset_pairs)
            .ok_or_else(overflow_error)?;
        let static_key_ceiling = static_pair_ceiling
            .checked_mul(u64::try_from(intervals.len()).map_err(|_| overflow_error())?)
            .ok_or_else(overflow_error)?;

        let sum_caps = caps.values().try_fold(0_u64, |total, cap| {
            total
                .checked_add(u64::try_from(*cap).map_err(|_| overflow_error())?)
                .ok_or_else(overflow_error)
        })?;
        let shard_count = u64::try_from(shard_count).map_err(|_| overflow_error())?;
        let pairs_per_shard = static_pair_ceiling
            .checked_add(shard_count - 1)
            .ok_or_else(overflow_error)?
            / shard_count;
        let worst_case_retained_candles_per_shard = pairs_per_shard
            .checked_mul(sum_caps)
            .ok_or_else(overflow_error)?;
        let estimated_memory_bytes = worst_case_retained_candles_per_shard
            .checked_mul(ESTIMATED_BYTES_PER_CLOSED_CANDLE)
            .ok_or_else(overflow_error)?;

        Ok(Self {
            static_pair_ceiling,
            static_key_ceiling,
            worst_case_retained_candles_per_shard,
            estimated_memory_bytes,
        })
    }
}

fn format_caps(caps: &BTreeMap<CandleInterval, usize>) -> String {
    caps.iter()
        .map(|(interval, cap)| format!("{}={cap}", interval.label()))
        .collect::<Vec<_>>()
        .join(",")
}

fn overflow_error() -> BpError {
    BpError::config_invalid(
        AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL,
        "retention calculation overflowed",
    )
}

pub(super) fn parse_correction_cutoff(
    name: &str,
    value: u64,
    allowed_lateness: Duration,
) -> BpResult<DurationMilliseconds> {
    if value == 0 {
        return Err(BpError::config_invalid(name, "must be greater than zero"));
    }
    if Duration::from_millis(value) < allowed_lateness {
        return Err(BpError::config_invalid(
            name,
            format!("must be at least AGGREGATOR_ALLOWED_LATENESS_MS ({allowed_lateness:?})"),
        ));
    }
    let value =
        i64::try_from(value).map_err(|error| BpError::config_invalid_source(name, error))?;
    Ok(DurationMilliseconds(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intervals() -> Vec<CandleInterval> {
        vec![
            CandleInterval::from_label("1m").unwrap(),
            CandleInterval::from_label("5m").unwrap(),
        ]
    }

    #[test]
    fn cap_parsing_requires_one_positive_entry_per_interval() {
        let intervals = intervals();
        assert_eq!(
            parse_max_closed_candles_per_interval("caps", "1m=1440,5m=288", &intervals,)
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
                parse_max_closed_candles_per_interval("caps", value, &intervals).is_err(),
                "{value} should be rejected"
            );
        }
    }

    #[test]
    fn retention_limits_use_the_full_static_pair_ceiling() {
        let intervals = intervals();
        let caps = BTreeMap::from([(intervals[0], 2), (intervals[1], 3)]);

        let limits = RetentionLimits::calculate(&intervals, &caps, 1).unwrap();

        let asset_id_count = u64::try_from(AssetId::COUNT).unwrap();
        let cex_id_count = u64::try_from(CexId::COUNT).unwrap();
        let static_pair_ceiling = cex_id_count * asset_id_count * asset_id_count;
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

        assert!(validate_retention(&intervals, &caps, 1, Duration::ZERO, 1_000, required,).is_ok());
        assert!(
            validate_retention(&intervals, &caps, 1, Duration::ZERO, 1_000, required - 1,).is_err()
        );
    }

    #[test]
    fn rejection_checks_cutoff_and_overflow() {
        let intervals = intervals();
        let caps = BTreeMap::from([(intervals[0], 1), (intervals[1], 1)]);

        assert!(
            validate_retention(
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
            validate_retention(
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
            validate_retention(
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
            validate_retention(&intervals, &caps, 1, Duration::from_millis(1_000), 999, 1,)
                .is_err()
        );
        assert!(validate_retention(&intervals, &caps, 1, Duration::ZERO, 0, 1,).is_err());

        let overflow_caps =
            BTreeMap::from([(intervals[0], usize::MAX), (intervals[1], usize::MAX)]);
        assert!(RetentionLimits::calculate(&intervals, &overflow_caps, 1).is_err());
    }
}
