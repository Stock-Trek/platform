use metrics::{describe_counter, describe_gauge, describe_histogram};

pub const INVOCATIONS_TOTAL: &str = "invocations_total";
pub const ERRORS_TOTAL: &str = "errors_total";
pub const IGNORED_EVENTS_TOTAL: &str = "ignored_events_total";
pub const DUPLICATE_EVENTS_TOTAL: &str = "duplicate_events_total";
pub const DURATION_SECONDS: &str = "duration_seconds";
pub const COLD_STARTS_TOTAL: &str = "cold_starts_total";
pub const ACTIVE_REQUESTS: &str = "active_requests";
pub const SECRET_CACHE_MISSES_TOTAL: &str = "secret_cache_misses_total";
pub const SECRET_LOAD_ATTEMPTS_TOTAL: &str = "secret_load_attempts_total";
pub const SECRET_LOAD_FAILURES_TOTAL: &str = "secret_load_failures_total";
pub const SECRET_LOAD_BACKOFF_HITS_TOTAL: &str = "secret_load_backoff_hits_total";
pub const SECRET_LOAD_IN_FLIGHT_WAITS_TOTAL: &str = "secret_load_in_flight_waits_total";
pub const SECRET_LOAD_SUCCESSES_TOTAL: &str = "secret_load_successes_total";
pub const MESSAGES_SENT_TOTAL: &str = "messages_sent_total";
pub const MESSAGES_FAILED_TOTAL: &str = "messages_failed_total";
pub const DUPLICATE_MESSAGES_TOTAL: &str = "duplicate_messages_total";
pub const BUILD_OUTCOMES_TOTAL: &str = "build_outcomes_total";
pub const BUILD_DURATION_SECONDS: &str = "build_duration_seconds";
pub const MARKET_DATA_JOB_OUTCOMES_TOTAL: &str = "market_data_job_outcomes_total";
pub const MARKET_DATA_JOB_DURATION_SECONDS: &str = "market_data_job_duration_seconds";
pub const MARKET_DATA_LOCK_CONTENTIONS_TOTAL: &str = "market_data_lock_contentions_total";
pub const MARKET_DATA_LOCK_SKIPS_TOTAL: &str = "market_data_lock_skips_total";
pub const MARKET_DATA_LOCK_LOST_TOTAL: &str = "market_data_lock_lost_total";
pub const MARKET_DATA_INGEST_WINDOWS_TOTAL: &str = "market_data_ingest_windows_total";
pub const MARKET_DATA_INGEST_GAPS_TOTAL: &str = "market_data_ingest_gaps_total";
pub const MARKET_DATA_INGEST_MISSING_CANDLES_TOTAL: &str =
    "market_data_ingest_missing_candles_total";
pub const MARKET_DATA_INGEST_SKIPPED_WINDOWS_TOTAL: &str =
    "market_data_ingest_skipped_windows_total";
pub const MARKET_DATA_COMPACTION_OUTCOMES_TOTAL: &str = "market_data_compaction_outcomes_total";
pub const MARKET_DATA_COMPACTION_DURATION_SECONDS: &str = "market_data_compaction_duration_seconds";
pub const MARKET_DATA_DROPPED_EVENTS_TOTAL: &str = "market_data_dropped_events_total";
pub const MARKET_DATA_DECODE_FAILURES_TOTAL: &str = "market_data_decode_failures_total";
pub const MARKET_DATA_BACKFILL_GAPS_TOTAL: &str = "market_data_backfill_gaps_total";
pub const MARKET_DATA_BACKFILL_GAP_EVENTS_EXPECTED_TOTAL: &str =
    "market_data_backfill_gap_events_expected_total";
pub const MARKET_DATA_BACKFILL_GAP_EVENTS_PUBLISHED_TOTAL: &str =
    "market_data_backfill_gap_events_published_total";
pub const MARKET_DATA_BACKFILL_GAP_COVERAGE_RATIO: &str = "market_data_backfill_gap_coverage_ratio";
pub const MARKET_DATA_ARCHIVE_FLUSHES_TOTAL: &str = "market_data_archive_flushes_total";
pub const MARKET_DATA_ARCHIVE_FLUSH_RECORDS_TOTAL: &str = "market_data_archive_flush_records_total";
pub const MARKET_DATA_ARCHIVE_FLUSH_DURATION_SECONDS: &str =
    "market_data_archive_flush_duration_seconds";
pub const MARKET_DATA_S3_RETRIES_TOTAL: &str = "market_data_s3_retries_total";

pub const STATUS_SUCCESS: &str = "success";
pub const STATUS_ERROR: &str = "error";
pub const STATUS_INCOMPLETE: &str = "incomplete";
pub const STATUS_PERMANENT_FAILURE: &str = "permanent_failure";
pub const STATUS_RETRYABLE_FAILURE: &str = "retryable_failure";

pub const INGEST_OUTCOME_INGESTED: &str = "ingested";
pub const INGEST_OUTCOME_SKIPPED_INCOMPLETE: &str = "skipped_incomplete";
pub const INGEST_OUTCOME_ERROR: &str = "error";
pub const INGEST_SKIP_REASON_COMPACTED: &str = "compacted";
pub const INGEST_SKIP_REASON_DEADLINE: &str = "deadline";

pub const STAGE_INGESTER: &str = "ingester";
pub const STAGE_AGGREGATOR: &str = "aggregator";
pub const STAGE_BACKFILLER: &str = "backfiller";
pub const STAGE_ARCHIVER: &str = "archiver";

pub const DROP_REASON_DUPLICATE: &str = "duplicate";
pub const DROP_REASON_OUT_OF_ORDER: &str = "out_of_order";
pub const DROP_REASON_NO_DECODER: &str = "no_decoder";
pub const DROP_REASON_CHANNEL_CLOSED: &str = "channel_closed";
pub const DROP_REASON_INVALID_TRADE: &str = "invalid_trade";

pub const DECODE_KIND_FRAME: &str = "frame";
pub const DECODE_KIND_MARKET_EVENT: &str = "market_event";
pub const DECODE_KIND_CLOSED_CANDLE: &str = "closed_candle";
pub const DECODE_KIND_TRADE: &str = "trade";
pub const DECODE_KIND_GAP_MARKER: &str = "gap_marker";

pub const BACKFILL_OUTCOME_COVERED: &str = "covered";
pub const BACKFILL_OUTCOME_PARTIAL: &str = "partial";
pub const BACKFILL_OUTCOME_EMPTY: &str = "empty";
pub const BACKFILL_OUTCOME_ERROR: &str = "error";

pub const FLUSH_REASON_SIZE: &str = "size";
pub const FLUSH_REASON_INTERVAL: &str = "interval";
pub const FLUSH_REASON_SHUTDOWN: &str = "shutdown";

pub const S3_OPERATION_ARCHIVE_UPLOAD: &str = "archive_upload";

pub struct DescriptionsMetrics;

impl DescriptionsMetrics {
    pub fn describe() {
        describe_counter!(
            INVOCATIONS_TOTAL,
            "Total number of Lambda invocations, labelled by route and status"
        );
        describe_counter!(
            ERRORS_TOTAL,
            "Total number of Lambda invocation errors, labelled by route and error type"
        );
        describe_counter!(
            IGNORED_EVENTS_TOTAL,
            "Total number of webhook events ignored, labelled by event and action"
        );
        describe_counter!(
            DUPLICATE_EVENTS_TOTAL,
            "Total number of webhook deliveries dropped as duplicates"
        );
        describe_histogram!(
            DURATION_SECONDS,
            "Duration of Lambda invocations in seconds, labelled by route"
        );
        describe_counter!(COLD_STARTS_TOTAL, "Total number of Lambda cold starts");
        describe_gauge!(
            ACTIVE_REQUESTS,
            "Number of Lambda requests currently being handled"
        );
        describe_counter!(
            SECRET_CACHE_MISSES_TOTAL,
            "Total number of secret cache misses"
        );
        describe_counter!(
            SECRET_LOAD_ATTEMPTS_TOTAL,
            "Total number of secret load attempts"
        );
        describe_counter!(
            SECRET_LOAD_FAILURES_TOTAL,
            "Total number of failed secret loads, labelled by error type"
        );
        describe_counter!(
            SECRET_LOAD_BACKOFF_HITS_TOTAL,
            "Total number of secret load attempts skipped due to failure backoff"
        );
        describe_counter!(
            SECRET_LOAD_IN_FLIGHT_WAITS_TOTAL,
            "Total number of requests that waited for an in-flight secret load"
        );
        describe_counter!(
            SECRET_LOAD_SUCCESSES_TOTAL,
            "Total number of successful secret loads"
        );
        describe_counter!(
            MESSAGES_SENT_TOTAL,
            "Total number of individual SQS messages sent successfully"
        );
        describe_counter!(
            MESSAGES_FAILED_TOTAL,
            "Total number of individual SQS messages that failed to send"
        );
        describe_counter!(
            DUPLICATE_MESSAGES_TOTAL,
            "Total number of SQS messages dropped as duplicates"
        );
        describe_counter!(
            BUILD_OUTCOMES_TOTAL,
            "Total number of build outcomes, labelled by status and error type"
        );
        describe_histogram!(
            BUILD_DURATION_SECONDS,
            "Duration of builds in seconds, labelled by status and error type"
        );
        describe_counter!(
            MARKET_DATA_JOB_OUTCOMES_TOTAL,
            "Total number of market data jobs, labelled by mode and status"
        );
        describe_histogram!(
            MARKET_DATA_JOB_DURATION_SECONDS,
            "Duration of market data jobs in seconds, labelled by mode and status"
        );
        describe_counter!(
            MARKET_DATA_LOCK_CONTENTIONS_TOTAL,
            "Total number of attempts that found the market data lock held by another invocation"
        );
        describe_counter!(
            MARKET_DATA_LOCK_SKIPS_TOTAL,
            "Total number of market data jobs skipped because another invocation held the lock"
        );
        describe_counter!(
            MARKET_DATA_LOCK_LOST_TOTAL,
            "Total number of market data locks lost to another invocation"
        );
        describe_counter!(
            MARKET_DATA_INGEST_WINDOWS_TOTAL,
            "Total number of market data ingest windows, labelled by outcome"
        );
        describe_counter!(
            MARKET_DATA_INGEST_GAPS_TOTAL,
            "Total number of market data windows with incomplete candle data"
        );
        describe_counter!(
            MARKET_DATA_INGEST_MISSING_CANDLES_TOTAL,
            "Total number of candles missing from incomplete market data windows"
        );
        describe_counter!(
            MARKET_DATA_INGEST_SKIPPED_WINDOWS_TOTAL,
            "Total number of market data windows skipped before ingest, labelled by reason"
        );
        describe_counter!(
            MARKET_DATA_COMPACTION_OUTCOMES_TOTAL,
            "Total number of market data compaction targets, labelled by status"
        );
        describe_histogram!(
            MARKET_DATA_COMPACTION_DURATION_SECONDS,
            "Duration of market data compaction targets in seconds, labelled by status"
        );
        describe_counter!(
            MARKET_DATA_DROPPED_EVENTS_TOTAL,
            "Total number of market data events dropped, labelled by stage and reason"
        );
        describe_counter!(
            MARKET_DATA_DECODE_FAILURES_TOTAL,
            "Total number of market data payloads that failed to decode, labelled by stage and kind"
        );
        describe_counter!(
            MARKET_DATA_BACKFILL_GAPS_TOTAL,
            "Total number of gap markers processed by the backfiller, labelled by outcome"
        );
        describe_counter!(
            MARKET_DATA_BACKFILL_GAP_EVENTS_EXPECTED_TOTAL,
            "Total number of events expected by processed gap markers"
        );
        describe_counter!(
            MARKET_DATA_BACKFILL_GAP_EVENTS_PUBLISHED_TOTAL,
            "Total number of events published while backfilling gap markers"
        );
        describe_histogram!(
            MARKET_DATA_BACKFILL_GAP_COVERAGE_RATIO,
            "Fraction of each gap marker covered by published backfill events"
        );
        describe_counter!(
            MARKET_DATA_ARCHIVE_FLUSHES_TOTAL,
            "Total number of archive flushes, labelled by trigger reason and status"
        );
        describe_counter!(
            MARKET_DATA_ARCHIVE_FLUSH_RECORDS_TOTAL,
            "Total number of records written by successful archive flushes"
        );
        describe_histogram!(
            MARKET_DATA_ARCHIVE_FLUSH_DURATION_SECONDS,
            "Duration of archive flushes in seconds, labelled by trigger reason and status"
        );
        describe_counter!(
            MARKET_DATA_S3_RETRIES_TOTAL,
            "Total number of S3 upload retries, labelled by operation"
        );
    }
}
