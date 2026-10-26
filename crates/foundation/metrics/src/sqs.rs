use crate::descriptions::*;
use metrics::counter;

pub struct SqsMetrics;

impl SqsMetrics {
    pub fn record_messages_sent(count: usize) {
        if count > 0 {
            counter!(MESSAGES_SENT_TOTAL).increment(count as u64);
        }
    }

    pub fn record_messages_failed(count: usize) {
        if count > 0 {
            counter!(MESSAGES_FAILED_TOTAL).increment(count as u64);
        }
    }

    pub fn record_duplicate_message() {
        counter!(DUPLICATE_MESSAGES_TOTAL).increment(1);
    }
}
