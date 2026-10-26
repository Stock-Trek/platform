use crate::descriptions::*;
use metrics::counter;

pub struct EventMetrics;

impl EventMetrics {
    pub fn record_ignored_event(event: &str, action: &str) {
        counter!(
            IGNORED_EVENTS_TOTAL,
            "event" => event.to_owned(),
            "action" => action.to_owned(),
        )
        .increment(1);
    }

    pub fn record_duplicate_event() {
        counter!(DUPLICATE_EVENTS_TOTAL).increment(1);
    }
}
