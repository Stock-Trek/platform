use crate::descriptions::*;
use metrics::{counter, gauge};

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
