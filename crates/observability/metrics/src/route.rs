use crate::descriptions::*;
use metrics::{counter, gauge, histogram};
use std::time::Instant;

pub struct RouteMetrics {
    route: &'static str,
    started_at: Instant,
}

impl RouteMetrics {
    pub fn start(route: &'static str) -> Self {
        gauge!(ACTIVE_REQUESTS).increment(1.0);
        Self {
            route,
            started_at: Instant::now(),
        }
    }

    pub fn record_success(&self) {
        self.record_duration();
        counter!(
            INVOCATIONS_TOTAL,
            "route" => self.route,
            "status" => STATUS_SUCCESS,
        )
        .increment(1);
    }
    pub fn record_success_status(&self, status: u16) {
        self.record_duration();
        counter!(
            INVOCATIONS_TOTAL,
            "route" => self.route,
            "status" => STATUS_SUCCESS,
            "http_status" => status.to_string(),
        )
        .increment(1);
    }

    pub fn record_error(&self, error_type: &'static str) {
        self.record_duration();
        counter!(
            INVOCATIONS_TOTAL,
            "route" => self.route,
            "status" => STATUS_ERROR,
        )
        .increment(1);
        counter!(
            ERRORS_TOTAL,
            "route" => self.route,
            "error_type" => error_type,
        )
        .increment(1);
    }
    pub fn record_error_status(&self, error_type: &'static str, status: u16) {
        self.record_duration();
        counter!(
            INVOCATIONS_TOTAL,
            "route" => self.route,
            "status" => STATUS_ERROR,
            "http_status" => status.to_string(),
        )
        .increment(1);
        counter!(
            ERRORS_TOTAL,
            "route" => self.route,
            "error_type" => error_type,
            "http_status" => status.to_string(),
        )
        .increment(1);
    }

    fn record_duration(&self) {
        histogram!(DURATION_SECONDS, "route" => self.route)
            .record(self.started_at.elapsed().as_secs_f64());
    }
}

impl Drop for RouteMetrics {
    fn drop(&mut self) {
        gauge!(ACTIVE_REQUESTS).decrement(1.0);
    }
}
