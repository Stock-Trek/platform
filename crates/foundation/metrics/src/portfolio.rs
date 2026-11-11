use crate::descriptions::*;
use metrics::{counter, gauge, histogram};
use std::time::Duration;

pub struct PortfolioMetrics;

impl PortfolioMetrics {
    pub fn record_connection_state(cex: &'static str, connected: bool) {
        gauge!(
            PORTFOLIO_CONNECTOR_CONNECTED,
            "cex" => cex,
        )
        .set(if connected { 1.0 } else { 0.0 });
    }

    pub fn record_subscriptions(cex: &'static str, subscriptions: usize) {
        gauge!(
            PORTFOLIO_CONNECTOR_SUBSCRIPTIONS,
            "cex" => cex,
        )
        .set(subscriptions as f64);
    }

    pub fn record_reconnect(cex: &'static str) {
        counter!(
            PORTFOLIO_CONNECTOR_RECONNECTS_TOTAL,
            "cex" => cex,
        )
        .increment(1);
    }

    pub fn record_connector_update_age(cex: &'static str, age: Duration) {
        gauge!(
            PORTFOLIO_CONNECTOR_LAST_UPDATE_AGE_SECONDS,
            "cex" => cex,
        )
        .set(age.as_secs_f64());
    }

    pub fn record_update_outcome(cex: &'static str, outcome: &'static str) {
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

    pub fn record_order_rejection(reason: &'static str) {
        counter!(
            PORTFOLIO_ORDER_REJECTIONS_TOTAL,
            "reason" => reason,
        )
        .increment(1);
    }
}
