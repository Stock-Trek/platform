use super::env::{
    AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES, AGGREGATOR_CORRECTION_CUTOFF_MS,
    AGGREGATOR_INTERVALS, AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY, AGGREGATOR_SHARD_COUNT,
};
use platform_errors::{BpError, BpResult};
use platform_types::new_types::DurationMilliseconds;

/// Number of variants in `stock_trek_types::cex::cex_id::CexId`.
///
/// Keep these cardinalities beside the validation code. The current
/// `stock-trek-types` release does not expose `EnumCount`/`EnumIter`, so update
/// both constants when the dependency's enum sets change. The ceiling deliberately uses the full
/// enum cardinality rather than only the keys that have traded so far.
const CEX_ID_COUNT: u64 = 1;
/// Number of variants in `stock_trek_types::cex::asset_id::AssetId`.
const ASSET_ID_COUNT: u64 = 28;

/// Conservative hot-memory estimate for one retained closed candle.
///
/// The candle, its key, the `Vec` element, the `HashMap` entry, and allocator
/// bookkeeping are all included. The value intentionally leaves a wide margin
/// over the serialized candle size so the startup check does not depend on the
/// exact layout of dependency types.
const ESTIMATED_CLOSED_CANDLE_BYTES: u64 = 1_024;

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

/// Validates the closed-candle retention policy and its worst-case memory use.
pub(super) fn validate_closed_candle_retention(
    max_closed_candles_per_key: usize,
    correction_cutoff: DurationMilliseconds,
    allowed_lateness: DurationMilliseconds,
    interval_count: usize,
    shard_count: usize,
    memory_budget_bytes: u64,
) -> BpResult<()> {
    if max_closed_candles_per_key == 0 {
        return Err(BpError::config_invalid(
            AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY,
            "must be greater than zero",
        ));
    }
    if correction_cutoff.0 <= 0 {
        return Err(BpError::config_invalid(
            AGGREGATOR_CORRECTION_CUTOFF_MS,
            "must be greater than zero",
        ));
    }
    if correction_cutoff < allowed_lateness {
        return Err(BpError::config_invalid(
            AGGREGATOR_CORRECTION_CUTOFF_MS,
            "must not be shorter than the allowed-lateness window",
        ));
    }

    let total_keys = static_candle_key_ceiling(interval_count)?;
    let max_closed_candles_per_key =
        u64::try_from(max_closed_candles_per_key).map_err(|error| {
            BpError::config_invalid_source(AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY, error)
        })?;
    let worst_case = worst_case_closed_candle_memory_bytes(
        total_keys,
        u64::try_from(shard_count)
            .map_err(|error| BpError::config_invalid_source(AGGREGATOR_SHARD_COUNT, error))?,
        max_closed_candles_per_key,
    )?;
    if worst_case > memory_budget_bytes {
        return Err(BpError::config_invalid(
            AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES,
            format!(
                "worst-case closed-candle memory is {worst_case} bytes but the budget is \
                 {memory_budget_bytes} bytes; total_keys={total_keys}, shard_count={shard_count}, \
                 {AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY}={max_closed_candles_per_key}, \
                 estimated_bytes_per_candle={ESTIMATED_CLOSED_CANDLE_BYTES}"
            ),
        ));
    }
    Ok(())
}

/// Static upper bound on the number of distinct candle keys.
///
/// This is conservative because it includes invalid same-asset pairs.
fn static_candle_key_ceiling(interval_count: usize) -> BpResult<u64> {
    let asset_pairs = ASSET_ID_COUNT
        .checked_mul(ASSET_ID_COUNT)
        .ok_or_else(key_ceiling_overflow)?;
    let keys_per_interval = CEX_ID_COUNT
        .checked_mul(asset_pairs)
        .ok_or_else(key_ceiling_overflow)?;
    let interval_count = u64::try_from(interval_count)
        .map_err(|error| BpError::config_invalid_source(AGGREGATOR_INTERVALS, error))?;
    keys_per_interval
        .checked_mul(interval_count)
        .ok_or_else(key_ceiling_overflow)
}

fn key_ceiling_overflow() -> BpError {
    BpError::config_invalid(
        AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES,
        "closed-candle key ceiling overflowed while validating the memory budget",
    )
}

/// Worst-case closed-candle memory for one shard, rounded up to whole keys.
fn worst_case_closed_candle_memory_bytes(
    total_keys: u64,
    shard_count: u64,
    max_closed_candles_per_key: u64,
) -> BpResult<u64> {
    if shard_count == 0 {
        return Err(BpError::config_invalid(
            AGGREGATOR_SHARD_COUNT,
            "must be greater than zero",
        ));
    }
    let keys_per_shard = total_keys
        .checked_add(shard_count - 1)
        .and_then(|rounded| rounded.checked_div(shard_count))
        .ok_or_else(key_ceiling_overflow)?;
    keys_per_shard
        .checked_mul(max_closed_candles_per_key)
        .and_then(|candles| candles.checked_mul(ESTIMATED_CLOSED_CANDLE_BYTES))
        .ok_or_else(key_ceiling_overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retention(
        max: usize,
        cutoff: i64,
        lateness: i64,
        intervals: usize,
        shards: usize,
        budget: u64,
    ) -> BpResult<()> {
        validate_closed_candle_retention(
            max,
            DurationMilliseconds(cutoff),
            DurationMilliseconds(lateness),
            intervals,
            shards,
            budget,
        )
    }

    fn worst_case(intervals: usize, shards: usize, max: usize) -> u64 {
        worst_case_closed_candle_memory_bytes(
            static_candle_key_ceiling(intervals).unwrap(),
            shards as u64,
            max as u64,
        )
        .unwrap()
    }

    #[test]
    fn rejects_zero_retention_cutoff_and_cutoff_shorter_than_lateness() {
        assert!(retention(0, 10, 0, 1, 1, u64::MAX).is_err());
        assert!(retention(1, 0, 0, 1, 1, u64::MAX).is_err());
        assert!(retention(1, 9, 10, 1, 1, u64::MAX).is_err());
        assert!(retention(1, 10, 10, 1, 1, u64::MAX).is_ok());
    }

    #[test]
    fn static_key_ceiling_uses_full_enum_cardinality_and_intervals() {
        assert_eq!(CEX_ID_COUNT, 1);
        assert_eq!(ASSET_ID_COUNT, 28);
        assert_eq!(static_candle_key_ceiling(1).unwrap(), 28 * 28);
        assert_eq!(static_candle_key_ceiling(3).unwrap(), 28 * 28 * 3);
    }

    #[test]
    fn accepts_a_budget_that_is_exactly_large_enough_and_rejects_one_byte_less() {
        let required = worst_case(2, 3, 10);
        assert!(retention(10, 10, 0, 2, 3, required).is_ok());
        assert!(retention(10, 10, 0, 2, 3, required - 1).is_err());
    }

    #[test]
    fn uses_the_full_static_key_ceiling_not_observed_keys() {
        // No runtime key count is an input to the calculation, so an
        // otherwise empty state still reserves for every possible key.
        let required = worst_case(1, 1, 5);
        assert!(retention(5, 10, 0, 1, 1, required).is_ok());
        assert!(retention(5, 10, 0, 1, 1, required - 1).is_err());
    }

    #[test]
    fn checked_arithmetic_reports_overflow_instead_of_wrapping() {
        assert!(worst_case_closed_candle_memory_bytes(u64::MAX, 1, u64::MAX).is_err());
        assert!(static_candle_key_ceiling(usize::MAX).is_err());
    }
}
