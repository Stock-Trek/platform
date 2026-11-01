use super::super::aggregator::AggregatorConfig;
use super::super::env::{
    AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES, AGGREGATOR_CORRECTION_CUTOFF_MS,
    AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::{AlignedWindow, parse_interval};
use platform_types::new_types::DurationMilliseconds;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

pub(in crate::config) const ESTIMATED_BYTES_PER_CLOSED_CANDLE: u64 = 512;

// `stock-trek-types` 0.5 exposes these enums as `#[non_exhaustive]` without
// `EnumCount`, so keep the cardinalities used by the static memory ceiling
// explicit. Update them when stock-trek adds an asset or exchange.
pub(in crate::config) const ASSET_ID_COUNT: u64 = 28;
pub(in crate::config) const CEX_ID_COUNT: u64 = 1;

impl AggregatorConfig {
    pub(in crate::config) fn validate_kv_bucket(name: &str, bucket: &str) -> BpResult<()> {
        let valid = !bucket.is_empty()
            && bucket.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            });
        if valid {
            Ok(())
        } else {
            Err(BpError::config_invalid(
                name,
                "must only contain alphanumeric characters, dashes, and underscores",
            ))
        }
    }

    pub(in crate::config) fn parse_max_closed_candles_per_interval(
        name: &str,
        value: &str,
        intervals: &[AlignedWindow],
    ) -> BpResult<BTreeMap<AlignedWindow, usize>> {
        let mut caps = BTreeMap::new();
        for entry in value.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            let Some((label, count)) = entry.split_once('=') else {
                return Err(BpError::config_invalid(
                    name,
                    format!("entry {entry:?} must use <interval>=<count>"),
                ));
            };
            let interval = parse_interval(label.trim())
                .map_err(|error| BpError::config_invalid_source(name, error))?;
            if !intervals.contains(&interval) {
                return Err(BpError::config_invalid(
                    name,
                    format!("interval {} is not an AlignedWindow interval", interval),
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
}

impl AggregatorConfig {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::config) fn validate_retention(
        intervals: &[AlignedWindow],
        caps: &BTreeMap<AlignedWindow, usize>,
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
                    RetentionLimits::format_caps(caps),
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::config) struct RetentionLimits {
    pub static_pair_ceiling: u64,
    pub static_key_ceiling: u64,
    pub worst_case_retained_candles_per_shard: u64,
    pub estimated_memory_bytes: u64,
}

impl RetentionLimits {
    pub(in crate::config) fn calculate(
        intervals: &[AlignedWindow],
        caps: &BTreeMap<AlignedWindow, usize>,
        shard_count: usize,
    ) -> BpResult<Self> {
        if shard_count == 0 {
            return Err(BpError::config_invalid(
                super::super::env::AGGREGATOR_SHARD_COUNT,
                "must be greater than zero",
            ));
        }
        let asset_pairs = ASSET_ID_COUNT
            .checked_mul(ASSET_ID_COUNT)
            .ok_or_else(Self::overflow_error)?;
        let static_pair_ceiling = CEX_ID_COUNT
            .checked_mul(asset_pairs)
            .ok_or_else(Self::overflow_error)?;
        let static_key_ceiling = static_pair_ceiling
            .checked_mul(u64::try_from(intervals.len()).map_err(|_| Self::overflow_error())?)
            .ok_or_else(Self::overflow_error)?;

        let sum_caps = caps.values().try_fold(0_u64, |total, cap| {
            total
                .checked_add(u64::try_from(*cap).map_err(|_| Self::overflow_error())?)
                .ok_or_else(Self::overflow_error)
        })?;
        let shard_count = u64::try_from(shard_count).map_err(|_| Self::overflow_error())?;
        let pairs_per_shard = static_pair_ceiling
            .checked_add(shard_count - 1)
            .ok_or_else(Self::overflow_error)?
            / shard_count;
        let worst_case_retained_candles_per_shard = pairs_per_shard
            .checked_mul(sum_caps)
            .ok_or_else(Self::overflow_error)?;
        let estimated_memory_bytes = worst_case_retained_candles_per_shard
            .checked_mul(ESTIMATED_BYTES_PER_CLOSED_CANDLE)
            .ok_or_else(Self::overflow_error)?;

        Ok(Self {
            static_pair_ceiling,
            static_key_ceiling,
            worst_case_retained_candles_per_shard,
            estimated_memory_bytes,
        })
    }
}

impl RetentionLimits {
    fn format_caps(caps: &BTreeMap<AlignedWindow, usize>) -> String {
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
}

impl AggregatorConfig {
    pub(in crate::config) fn parse_correction_cutoff(
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
}
