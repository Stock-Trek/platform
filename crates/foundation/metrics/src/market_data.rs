use crate::descriptions::*;
use metrics::{counter, gauge, histogram};
use std::time::Duration;

pub struct MarketDataMetrics;

impl MarketDataMetrics {
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

    pub fn record_lease_acquisition(scope: &str) {
        counter!(
            MARKET_DATA_LEASE_ACQUISITIONS_TOTAL,
            "scope" => scope.to_string(),
        )
        .increment(1);
    }

    pub fn record_lease_contention(scope: &str) {
        counter!(
            MARKET_DATA_LOCK_CONTENTIONS_TOTAL,
            "scope" => scope.to_string(),
        )
        .increment(1);
    }

    pub fn record_lease_skip(scope: &str) {
        counter!(
            MARKET_DATA_LOCK_SKIPS_TOTAL,
            "scope" => scope.to_string(),
        )
        .increment(1);
    }

    pub fn record_lease_lost(scope: &str, reason: &'static str) {
        counter!(
            MARKET_DATA_LOCK_LOST_TOTAL,
            "scope" => scope.to_string(),
            "reason" => reason,
        )
        .increment(1);
    }

    pub fn record_archive_commit(table: &'static str, outcome: &'static str) {
        counter!(
            MARKET_DATA_ARCHIVE_COMMIT_OUTCOMES_TOTAL,
            "table" => table,
            "outcome" => outcome,
        )
        .increment(1);
    }

    pub fn record_archive_recovery(table: &'static str, outcome: &'static str, objects: u64) {
        if objects == 0 {
            return;
        }
        counter!(
            MARKET_DATA_ARCHIVE_RECOVERY_OBJECTS_TOTAL,
            "table" => table,
            "outcome" => outcome,
        )
        .increment(objects);
    }

    pub fn record_archive_recovery_page(
        table: &'static str,
        files: u64,
        bytes: u64,
        remaining: u64,
    ) {
        if files > 0 {
            counter!(
                MARKET_DATA_ARCHIVE_RECOVERY_PAGE_FILES_TOTAL,
                "table" => table,
            )
            .increment(files);
        }
        if bytes > 0 {
            counter!(
                MARKET_DATA_ARCHIVE_RECOVERY_PAGE_BYTES_TOTAL,
                "table" => table,
            )
            .increment(bytes);
        }
        gauge!(
            MARKET_DATA_ARCHIVE_RECOVERY_REMAINING_OBJECTS,
            "table" => table,
        )
        .set(remaining as f64);
    }

    pub fn record_compactor_inventory(pending: usize, total: usize) {
        gauge!(MARKET_DATA_COMPACTOR_INVENTORY_OBJECTS).set(total as f64);
        gauge!(MARKET_DATA_COMPACTOR_PENDING_OBJECTS).set(pending as f64);
    }

    pub fn record_archive_listing_page(source: &'static str, objects: u64) {
        counter!(
            MARKET_DATA_ARCHIVE_LISTING_PAGES_TOTAL,
            "source" => source,
        )
        .increment(1);
        if objects > 0 {
            counter!(
                MARKET_DATA_ARCHIVE_LISTING_OBJECTS_TOTAL,
                "source" => source,
            )
            .increment(objects);
        }
    }

    pub fn record_index_page_read() {
        counter!(MARKET_DATA_ARCHIVE_INDEX_PAGES_READ_TOTAL).increment(1);
    }

    pub fn record_index_block_read() {
        counter!(MARKET_DATA_ARCHIVE_INDEX_BLOCKS_READ_TOTAL).increment(1);
    }

    pub fn record_index_cache_hit(kind: &'static str) {
        counter!(
            MARKET_DATA_ARCHIVE_INDEX_CACHE_HITS_TOTAL,
            "kind" => kind,
        )
        .increment(1);
    }

    pub fn record_index_cache_miss(kind: &'static str) {
        counter!(
            MARKET_DATA_ARCHIVE_INDEX_CACHE_MISSES_TOTAL,
            "kind" => kind,
        )
        .increment(1);
    }

    pub fn record_index_fallback_scan(table: &'static str) {
        counter!(
            MARKET_DATA_ARCHIVE_INDEX_FALLBACK_SCANS_TOTAL,
            "table" => table,
        )
        .increment(1);
    }

    pub fn record_compactor_poll(duration: Duration) {
        histogram!(MARKET_DATA_COMPACTOR_POLL_DURATION_SECONDS).record(duration.as_secs_f64());
    }

    pub fn record_compactor_backlog(age: Duration, growth: i64) {
        gauge!(MARKET_DATA_COMPACTOR_BACKLOG_AGE_SECONDS).set(age.as_secs_f64());
        gauge!(MARKET_DATA_COMPACTOR_BACKLOG_GROWTH).set(growth as f64);
    }

    pub fn record_compaction_outcome(kind: &'static str, status: &'static str) {
        counter!(
            MARKET_DATA_COMPACTION_OUTCOMES_TOTAL,
            "kind" => kind,
            "status" => status,
        )
        .increment(1);
    }

    pub fn record_maintenance_stage(stage: &'static str, status: &'static str, objects: u64) {
        counter!(
            "market_data_archive_maintenance_stage_outcomes_total",
            "stage" => stage,
            "status" => status,
        )
        .increment(1);
        if objects > 0 {
            counter!(
                "market_data_archive_maintenance_stage_objects_total",
                "stage" => stage,
                "status" => status,
            )
            .increment(objects);
        }
    }

    pub fn record_maintenance_stage_lag(stage: &str, lag: Duration) {
        gauge!(
            "market_data_archive_maintenance_stage_lag_seconds",
            "stage" => stage.to_string(),
        )
        .set(lag.as_secs_f64());
    }

    pub fn record_maintenance_alert(stage: &'static str, code: &str) {
        counter!(
            "market_data_archive_maintenance_alerts_total",
            "stage" => stage,
            "code" => code.to_string(),
        )
        .increment(1);
    }

    pub fn record_maintenance_lease_age(fencing_token: u64) {
        gauge!("market_data_archive_maintenance_lease_token").set(fencing_token as f64);
    }

    pub fn record_maintenance_partition_files(table: &str, files: u64) {
        gauge!(
            "market_data_archive_maintenance_partition_files",
            "table" => table.to_string(),
        )
        .set(files as f64);
    }

    pub fn record_maintenance_raw_backlog(kind: &str, age: Duration) {
        gauge!(
            "market_data_archive_maintenance_raw_backlog_seconds",
            "kind" => kind.to_string(),
        )
        .set(age.as_secs_f64());
    }

    pub fn record_recovery_layer(layer: &'static str, kind: &'static str) {
        counter!(
            "market_data_archive_recovery_layer_total",
            "layer" => layer,
            "kind" => kind,
        )
        .increment(1);
    }

    pub fn record_recovery_ledger_watermark(kind: &str, watermark_age_millis: i64) {
        gauge!(
            "market_data_archive_recovery_ledger_watermark_seconds",
            "kind" => kind.to_string(),
        )
        .set((watermark_age_millis.max(0) / 1_000) as f64);
    }

    pub fn record_maintenance_metadata_files(table: &str, files: u64) {
        gauge!(
            "market_data_archive_maintenance_metadata_files",
            "table" => table.to_string(),
        )
        .set(files as f64);
    }

    pub fn record_maintenance_run(status: &'static str) {
        counter!(
            "market_data_archive_maintenance_runs_total",
            "status" => status,
        )
        .increment(1);
    }

    pub fn record_raw_expiry_decision(decision: &'static str, kind: &'static str) {
        counter!(
            "market_data_archive_raw_expiry_decisions_total",
            "decision" => decision,
            "kind" => kind,
        )
        .increment(1);
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

    pub fn record_retained_closed_candles(shard: u32, interval: &str, candles: usize) {
        gauge!(
            MARKET_DATA_AGGREGATOR_RETAINED_CLOSED_CANDLES,
            "shard" => shard.to_string(),
            "interval" => interval.to_string(),
        )
        .set(candles as f64);
    }

    pub fn record_closed_candle_evictions(shard: u32, interval: &str, candles: u64) {
        if candles > 0 {
            counter!(
                MARKET_DATA_AGGREGATOR_CLOSED_CANDLES_EVICTED_TOTAL,
                "shard" => shard.to_string(),
                "interval" => interval.to_string(),
            )
            .increment(candles);
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
