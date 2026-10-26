use crate::descriptions::*;
use metrics::{counter, histogram};
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
}
