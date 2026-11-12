use crate::descriptions::*;
use metrics::{counter, gauge, histogram};
use std::time::Duration;

pub struct PortfolioMetrics;

impl PortfolioMetrics {
    pub fn record_connection_state(cex: impl Into<String>, connected: bool) {
        let cex = cex.into();
        gauge!(
            PORTFOLIO_CONNECTOR_CONNECTED,
            "cex" => cex,
        )
        .set(if connected { 1.0 } else { 0.0 });
    }

    pub fn record_subscriptions(cex: impl Into<String>, subscriptions: usize) {
        let cex = cex.into();
        gauge!(
            PORTFOLIO_CONNECTOR_SUBSCRIPTIONS,
            "cex" => cex,
        )
        .set(subscriptions as f64);
    }

    pub fn record_reconnect(cex: impl Into<String>) {
        let cex = cex.into();
        counter!(
            PORTFOLIO_CONNECTOR_RECONNECTS_TOTAL,
            "cex" => cex,
        )
        .increment(1);
    }

    pub fn record_connector_update_age(cex: impl Into<String>, age: Duration) {
        let cex = cex.into();
        gauge!(
            PORTFOLIO_CONNECTOR_LAST_UPDATE_AGE_SECONDS,
            "cex" => cex,
        )
        .set(age.as_secs_f64());
    }

    pub fn record_update_outcome(cex: impl Into<String>, outcome: &'static str) {
        let cex = cex.into();
        counter!(
            PORTFOLIO_UPDATES_TOTAL,
            "cex" => cex,
            "outcome" => outcome,
        )
        .increment(1);
    }

    pub fn record_consumer_lag(lag: Duration) {
        histogram!(PORTFOLIO_CONSUMER_LAG_SECONDS).record(lag.as_secs_f64());
    }

    pub fn record_ownership_lag(lag: Duration) {
        histogram!(PORTFOLIO_OWNERSHIP_LAG_SECONDS).record(lag.as_secs_f64());
    }

    pub fn record_ownership_outcome(outcome: &'static str) {
        counter!(
            PORTFOLIO_OWNERSHIP_OUTCOMES_TOTAL,
            "outcome" => outcome,
        )
        .increment(1);
    }

    pub fn record_apply_failure(stage: &'static str, operation: &'static str) {
        counter!(
            PORTFOLIO_APPLY_FAILURES_TOTAL,
            "stage" => stage,
            "operation" => operation,
        )
        .increment(1);
    }

    pub fn record_order_rejection(reason: &'static str) {
        counter!(
            PORTFOLIO_ORDER_REJECTIONS_TOTAL,
            "reason" => reason,
        )
        .increment(1);
    }
}
