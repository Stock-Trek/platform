use crate::descriptions::*;
use metrics::{counter, histogram};
use std::time::Duration;

pub struct BuildMetrics;

impl BuildMetrics {
    pub fn record_success(duration: Duration) {
        counter!(BUILD_OUTCOMES_TOTAL, "status" => STATUS_SUCCESS).increment(1);
        histogram!(BUILD_DURATION_SECONDS, "status" => STATUS_SUCCESS)
            .record(duration.as_secs_f64());
    }

    pub fn record_permanent_failure(error_type: &'static str, duration: Duration) {
        Self::record_failure(STATUS_PERMANENT_FAILURE, error_type, duration);
    }

    pub fn record_retryable_failure(error_type: &'static str, duration: Duration) {
        Self::record_failure(STATUS_RETRYABLE_FAILURE, error_type, duration);
    }

    fn record_failure(status: &'static str, error_type: &'static str, duration: Duration) {
        counter!(
            BUILD_OUTCOMES_TOTAL,
            "status" => status,
            "error_type" => error_type,
        )
        .increment(1);
        histogram!(
            BUILD_DURATION_SECONDS,
            "status" => status,
            "error_type" => error_type,
        )
        .record(duration.as_secs_f64());
    }
}
