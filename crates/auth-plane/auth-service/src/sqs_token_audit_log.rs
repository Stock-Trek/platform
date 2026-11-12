use crate::{TokenAuditEvent, TokenAuditLog};
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_sqs::Sqs;
use platform_time::Deadline;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

#[async_trait]
pub(crate) trait AuditQueue: Send + Sync {
    async fn deliver(&self, events: &[TokenAuditEvent], deadline: Deadline) -> BuildResult<()>;
}

#[async_trait]
impl AuditQueue for Sqs {
    async fn deliver(&self, events: &[TokenAuditEvent], deadline: Deadline) -> BuildResult<()> {
        self.send_json(events, deadline).await
    }
}

/// Delivers audit events to an SQS queue.
///
/// Unlike the in-memory log, this sink is durable. It is used by
/// [`crate::TokenAuditRelay`] to drain token outboxes; a failed send is returned
/// to the relay so the event remains pending for a later retry.
#[derive(Clone)]
pub struct SqsTokenAuditLog {
    queue: Arc<dyn AuditQueue>,
    timeout: Duration,
}

impl SqsTokenAuditLog {
    pub fn new(sqs: Sqs, timeout: Duration) -> Self {
        Self {
            queue: Arc::new(sqs),
            timeout,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_queue(queue: Arc<dyn AuditQueue>, timeout: Duration) -> Self {
        Self { queue, timeout }
    }

    fn deadline(&self) -> Deadline {
        Deadline::new(SystemTime::now() + self.timeout, Duration::ZERO)
    }
}

#[async_trait]
impl TokenAuditLog for SqsTokenAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> BuildResult<()> {
        self.queue
            .deliver(std::slice::from_ref(&event), self.deadline())
            .await
    }
}
