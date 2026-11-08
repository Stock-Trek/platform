use crate::{TokenAuditEvent, TokenAuditLog};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_sqs::sqs::Sqs;
use platform_time::deadline::Deadline;
use std::time::{Duration, SystemTime};

/// Delivers every audit event to an SQS queue before returning.
///
/// Unlike the in-memory log, this sink is durable and applies backpressure:
/// each auth operation waits for SQS to accept the event, and a failed send is
/// returned to the caller instead of silently dropping the event.
#[derive(Clone)]
pub struct SqsTokenAuditLog {
    sqs: Sqs,
    timeout: Duration,
}

impl SqsTokenAuditLog {
    pub fn new(sqs: Sqs, timeout: Duration) -> Self {
        Self { sqs, timeout }
    }
}

#[async_trait]
impl TokenAuditLog for SqsTokenAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        let deadline = Deadline::new(SystemTime::now() + self.timeout, Duration::ZERO);
        self.sqs
            .send_json(std::slice::from_ref(&event), deadline)
            .await
    }
}
