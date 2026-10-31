use crate::descriptions::*;
use metrics::{counter, gauge, histogram};
use std::time::Duration;

pub struct MarketDataMetrics;

impl MarketDataMetrics {
    pub fn record_job_success(mode: &'static str, incomplete: bool, duration: Duration) {
        let status = if incomplete {
            STATUS_INCOMPLETE
        } else {
            STATUS_SUCCESS
        };
        counter!(
            MARKET_DATA_JOB_OUTCOMES_TOTAL,
            "mode" => mode,
            "status" => status,
        )
        .increment(1);
        histogram!(
            MARKET_DATA_JOB_DURATION_SECONDS,
            "mode" => mode,
            "status" => status,
        )
        .record(duration.as_secs_f64());
    }

    pub fn record_job_error(mode: &'static str, error_type: &'static str, duration: Duration) {
        counter!(
            MARKET_DATA_JOB_OUTCOMES_TOTAL,
            "mode" => mode,
            "status" => STATUS_ERROR,
            "error_type" => error_type,
        )
        .increment(1);
        histogram!(
            MARKET_DATA_JOB_DURATION_SECONDS,
            "mode" => mode,
            "status" => STATUS_ERROR,
            "error_type" => error_type,
        )
        .record(duration.as_secs_f64());
    }

    pub fn record_lock_contention() {
        counter!(MARKET_DATA_LOCK_CONTENTIONS_TOTAL).increment(1);
    }

    pub fn record_lock_skip() {
        counter!(MARKET_DATA_LOCK_SKIPS_TOTAL).increment(1);
    }

    pub fn record_lock_lost() {
        counter!(MARKET_DATA_LOCK_LOST_TOTAL).increment(1);
    }

    pub fn record_ingest_ingested() {
        counter!(
            MARKET_DATA_INGEST_WINDOWS_TOTAL,
            "outcome" => INGEST_OUTCOME_INGESTED,
        )
        .increment(1);
    }

    pub fn record_ingest_gap(expected: usize, received: usize) {
        counter!(
            MARKET_DATA_INGEST_WINDOWS_TOTAL,
            "outcome" => INGEST_OUTCOME_SKIPPED_INCOMPLETE,
        )
        .increment(1);
        counter!(MARKET_DATA_INGEST_GAPS_TOTAL).increment(1);
        let missing = expected.saturating_sub(received);
        if missing > 0 {
            counter!(MARKET_DATA_INGEST_MISSING_CANDLES_TOTAL).increment(missing as u64);
        }
    }

    pub fn record_ingest_error(error_type: &'static str) {
        counter!(
            MARKET_DATA_INGEST_WINDOWS_TOTAL,
            "outcome" => INGEST_OUTCOME_ERROR,
            "error_type" => error_type,
        )
        .increment(1);
    }

    pub fn record_ingest_skipped(reason: &'static str, windows: usize) {
        if windows > 0 {
            counter!(
                MARKET_DATA_INGEST_SKIPPED_WINDOWS_TOTAL,
                "reason" => reason,
            )
            .increment(windows as u64);
        }
    }

    pub fn record_compaction_success(duration: Duration) {
        counter!(
            MARKET_DATA_COMPACTION_OUTCOMES_TOTAL,
            "status" => STATUS_SUCCESS,
        )
        .increment(1);
        histogram!(
            MARKET_DATA_COMPACTION_DURATION_SECONDS,
            "status" => STATUS_SUCCESS,
        )
        .record(duration.as_secs_f64());
    }

    pub fn record_compaction_failure(error_type: &'static str, duration: Duration) {
        counter!(
            MARKET_DATA_COMPACTION_OUTCOMES_TOTAL,
            "status" => STATUS_ERROR,
            "error_type" => error_type,
        )
        .increment(1);
        histogram!(
            MARKET_DATA_COMPACTION_DURATION_SECONDS,
            "status" => STATUS_ERROR,
            "error_type" => error_type,
        )
        .record(duration.as_secs_f64());
    }

    pub fn record_dropped_event(stage: &'static str, reason: &'static str) {
        counter!(
            MARKET_DATA_DROPPED_EVENTS_TOTAL,
            "stage" => stage,
            "reason" => reason,
        )
        .increment(1);
    }

    pub fn record_decode_failure(stage: &'static str, kind: &'static str) {
        counter!(
            MARKET_DATA_DECODE_FAILURES_TOTAL,
            "stage" => stage,
            "kind" => kind,
        )
        .increment(1);
    }

    pub fn record_backfill_gap_outcome(outcome: &'static str) {
        counter!(MARKET_DATA_BACKFILL_GAPS_TOTAL, "outcome" => outcome).increment(1);
    }

    pub fn record_backfill_gap_coverage(expected: u64, published: u64) {
        if expected == 0 {
            return;
        }
        counter!(MARKET_DATA_BACKFILL_GAP_EVENTS_EXPECTED_TOTAL).increment(expected);
        counter!(MARKET_DATA_BACKFILL_GAP_EVENTS_PUBLISHED_TOTAL).increment(published);
        let ratio = (published as f64 / expected as f64).clamp(0.0, 1.0);
        histogram!(MARKET_DATA_BACKFILL_GAP_COVERAGE_RATIO).record(ratio);
    }

    pub fn record_archive_flush(
        reason: &'static str,
        records: usize,
        success: bool,
        duration: Duration,
    ) {
        let status = if success {
            STATUS_SUCCESS
        } else {
            STATUS_ERROR
        };
        counter!(
            MARKET_DATA_ARCHIVE_FLUSHES_TOTAL,
            "reason" => reason,
            "status" => status,
        )
        .increment(1);
        histogram!(
            MARKET_DATA_ARCHIVE_FLUSH_DURATION_SECONDS,
            "reason" => reason,
            "status" => status,
        )
        .record(duration.as_secs_f64());
        if success && records > 0 {
            counter!(MARKET_DATA_ARCHIVE_FLUSH_RECORDS_TOTAL).increment(records as u64);
        }
    }

    pub fn record_s3_retry(operation: &'static str) {
        counter!(MARKET_DATA_S3_RETRIES_TOTAL, "operation" => operation).increment(1);
    }
}

/// Metrics specific to sharded candle aggregation.
pub struct AggregatorMetrics;

impl AggregatorMetrics {
    /// Records the age of the most recent successful checkpoint for a
    /// shard.
    pub fn record_checkpoint_age(shard: u32, age: Duration) {
        gauge!(
            MARKET_DATA_AGGREGATOR_CHECKPOINT_AGE_SECONDS,
            "shard" => shard.to_string(),
        )
        .set(age.as_secs_f64());
    }

    pub fn record_checkpoint_failure(shard: u32, error_type: &'static str) {
        counter!(
            MARKET_DATA_AGGREGATOR_CHECKPOINT_FAILURES_TOTAL,
            "shard" => shard.to_string(),
            "error_type" => error_type,
        )
        .increment(1);
    }

    /// Records how far the processor's event-time watermark trails the wall
    /// clock.
    pub fn record_watermark_lag(shard: u32, lag_seconds: f64) {
        gauge!(
            MARKET_DATA_AGGREGATOR_WATERMARK_LAG_SECONDS,
            "shard" => shard.to_string(),
        )
        .set(lag_seconds.max(0.0));
    }

    pub fn record_replay_events(shard: u32, source: &'static str, events: u64) {
        if events == 0 {
            return;
        }
        counter!(
            MARKET_DATA_AGGREGATOR_REPLAY_EVENTS_TOTAL,
            "shard" => shard.to_string(),
            "source" => source,
        )
        .increment(events);
    }

    pub fn record_replay_progress(shard: u32, source: &'static str, ratio: f64) {
        gauge!(
            MARKET_DATA_AGGREGATOR_REPLAY_PROGRESS_RATIO,
            "shard" => shard.to_string(),
            "source" => source,
        )
        .set(ratio.clamp(0.0, 1.0));
    }

    pub fn record_state_restore(shard: u32, outcome: &'static str, duration: Duration) {
        histogram!(
            MARKET_DATA_AGGREGATOR_STATE_RESTORE_SECONDS,
            "shard" => shard.to_string(),
            "outcome" => outcome,
        )
        .record(duration.as_secs_f64());
    }

    pub fn record_rebalance(shard: u32, reason: &'static str) {
        counter!(
            MARKET_DATA_AGGREGATOR_REBALANCES_TOTAL,
            "shard" => shard.to_string(),
            "reason" => reason,
        )
        .increment(1);
    }

    pub fn record_duplicate_events(events: u64) {
        if events > 0 {
            counter!(MARKET_DATA_AGGREGATOR_DUPLICATE_EVENTS_TOTAL).increment(events);
        }
    }

    pub fn record_incomplete_windows(windows: u64) {
        if windows > 0 {
            counter!(MARKET_DATA_AGGREGATOR_INCOMPLETE_WINDOWS_TOTAL).increment(windows);
        }
    }

    /// Records the number of closed candles currently retained for a shard.
    pub fn record_retained_closed_candles(shard: u32, candles: u64) {
        gauge!(
            MARKET_DATA_AGGREGATOR_RETAINED_CLOSED_CANDLES,
            "shard" => shard.to_string(),
        )
        .set(candles as f64);
    }

    pub fn record_closed_candles_evicted(candles: u64) {
        if candles > 0 {
            counter!(MARKET_DATA_AGGREGATOR_CLOSED_CANDLES_EVICTED_TOTAL).increment(candles);
        }
    }

    pub fn record_out_of_contract_trades(trades: u64) {
        if trades > 0 {
            counter!(MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_TRADES_TOTAL).increment(trades);
        }
    }

    pub fn record_out_of_contract_gap_windows(windows: u64) {
        if windows > 0 {
            counter!(MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_GAP_WINDOWS_TOTAL).increment(windows);
        }
    }
}
