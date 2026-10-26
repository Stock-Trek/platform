use crate::descriptions::*;
use metrics::counter;

pub struct SecretMetrics;

impl SecretMetrics {
    pub fn record_secret_cache_miss() {
        counter!(SECRET_CACHE_MISSES_TOTAL).increment(1);
    }

    pub fn record_secret_load_attempt() {
        counter!(SECRET_LOAD_ATTEMPTS_TOTAL).increment(1);
    }

    pub fn record_secret_load_failure(error_type: &'static str) {
        counter!(SECRET_LOAD_FAILURES_TOTAL, "error_type" => error_type).increment(1);
    }

    pub fn record_secret_load_backoff_hit() {
        counter!(SECRET_LOAD_BACKOFF_HITS_TOTAL).increment(1);
    }

    pub fn record_secret_load_in_flight_wait() {
        counter!(SECRET_LOAD_IN_FLIGHT_WAITS_TOTAL).increment(1);
    }

    pub fn record_secret_load_success() {
        counter!(SECRET_LOAD_SUCCESSES_TOTAL).increment(1);
    }
}
