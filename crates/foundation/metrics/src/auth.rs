use crate::descriptions::*;
use metrics::{counter, gauge, histogram};
use std::time::Duration;

pub struct AuthReplayMetrics;

impl AuthReplayMetrics {
    pub fn record_hit() {
        counter!(AUTH_REPLAY_HITS_TOTAL).increment(1);
    }

    pub fn record_eviction(reason: &'static str) {
        counter!(AUTH_REPLAY_EVICTIONS_TOTAL, "reason" => reason).increment(1);
    }

    pub fn record_store_failure(operation: &'static str) {
        counter!(AUTH_REPLAY_STORE_FAILURES_TOTAL, "operation" => operation).increment(1);
    }

    pub fn record_capacity_rejection(capacity: usize) {
        counter!(
            AUTH_REPLAY_CAPACITY_REJECTIONS_TOTAL,
            "capacity" => capacity.to_string(),
        )
        .increment(1);
    }

    pub fn record_size(size: usize) {
        gauge!(AUTH_REPLAY_ENTRIES).set(size as f64);
    }
}

pub struct AuthAuditRelayMetrics;

impl AuthAuditRelayMetrics {
    pub fn record_backlog(
        pending_events: usize,
        oldest_pending_age: Option<Duration>,
        pending_rotations: usize,
    ) {
        gauge!(AUTH_AUDIT_RELAY_PENDING_EVENTS).set(pending_events as f64);
        gauge!(AUTH_AUDIT_RELAY_OLDEST_PENDING_AGE_SECONDS)
            .set(oldest_pending_age.map_or(0.0, |age| age.as_secs_f64()));
        gauge!(AUTH_AUDIT_RELAY_PENDING_ROTATIONS).set(pending_rotations as f64);
    }

    pub fn record_delivered(count: usize) {
        if count > 0 {
            counter!(AUTH_AUDIT_RELAY_DELIVERED_TOTAL).increment(count as u64);
        }
    }

    pub fn record_failure(operation: &'static str, error_type: &'static str) {
        counter!(
            AUTH_AUDIT_RELAY_FAILURES_TOTAL,
            "operation" => operation,
            "error_type" => error_type,
        )
        .increment(1);
    }

    pub fn record_drain_duration(duration: Duration) {
        histogram!(AUTH_AUDIT_RELAY_DRAIN_DURATION_SECONDS).record(duration.as_secs_f64());
    }
}

pub struct AuthTokenCacheMetrics;

impl AuthTokenCacheMetrics {
    pub fn record_hit(result: &'static str) {
        counter!(AUTH_TOKEN_CACHE_HITS_TOTAL, "result" => result).increment(1);
    }

    pub fn record_miss() {
        counter!(AUTH_TOKEN_CACHE_MISSES_TOTAL).increment(1);
    }

    pub fn record_stale_serve() {
        counter!(AUTH_TOKEN_CACHE_STALE_SERVES_TOTAL).increment(1);
    }

    pub fn record_size(entries: usize, negative_entries: usize) {
        gauge!(AUTH_TOKEN_CACHE_ENTRIES).set(entries as f64);
        gauge!(AUTH_TOKEN_CACHE_NEGATIVE_ENTRIES).set(negative_entries as f64);
    }

    pub fn record_breaker_state(state: u8) {
        gauge!(AUTH_TOKEN_CACHE_BREAKER_STATE).set(state as f64);
    }

    pub fn record_breaker_transition(state: &'static str) {
        counter!(AUTH_TOKEN_CACHE_BREAKER_TRANSITIONS_TOTAL, "state" => state).increment(1);
    }

    pub fn record_error(operation: &'static str, error_type: &'static str) {
        counter!(
            AUTH_TOKEN_CACHE_ERRORS_TOTAL,
            "operation" => operation,
            "error_type" => error_type,
        )
        .increment(1);
    }

    pub fn record_vault_read(duration: Duration) {
        counter!(AUTH_TOKEN_CACHE_VAULT_READS_TOTAL).increment(1);
        histogram!(AUTH_TOKEN_CACHE_VAULT_READ_DURATION_SECONDS).record(duration.as_secs_f64());
    }
}

pub struct AuthUsageQueueMetrics;

impl AuthUsageQueueMetrics {
    pub fn record_enqueued() {
        counter!(AUTH_USAGE_QUEUE_ENQUEUED_TOTAL).increment(1);
    }

    pub fn record_capacity_rejection() {
        counter!(AUTH_USAGE_QUEUE_CAPACITY_REJECTIONS_TOTAL).increment(1);
    }

    pub fn record_delivered(count: usize) {
        if count > 0 {
            counter!(AUTH_USAGE_QUEUE_DELIVERED_TOTAL).increment(count as u64);
        }
    }

    pub fn record_failure(operation: &'static str, error_type: &'static str) {
        counter!(
            AUTH_USAGE_QUEUE_FAILURES_TOTAL,
            "operation" => operation,
            "error_type" => error_type,
        )
        .increment(1);
    }

    pub fn record_drain_duration(duration: Duration) {
        histogram!(AUTH_USAGE_QUEUE_DRAIN_DURATION_SECONDS).record(duration.as_secs_f64());
    }
}
