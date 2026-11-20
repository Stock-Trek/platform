use crate::{
    SqsTokenAuditLog, TokenAuditRelay, TokenStore, TokenUsageQueue, TokenUsageWorker,
    token_audit::TokenAuditLog,
};
use platform_aws::Aws;
use platform_errors::PlatformResult;
use platform_sqs::{Sqs, SqsConfig};
use platform_time::Timeouts;
use std::{sync::Arc, time::Duration};
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

/// Applies queued usage records and relays pending audit events to SQS.
///
/// Runs inside the auth service on a fixed interval. A failed cycle leaves
/// usage reservations and audit outbox entries pending for the next cycle, so
/// delivery stays at-least-once with stable event IDs.
pub(crate) struct TokenDrain {
    usage: TokenUsageWorker,
    relay: TokenAuditRelay,
    interval: Duration,
}

impl TokenDrain {
    pub(crate) fn new(
        store: Arc<dyn TokenStore>,
        sink: Arc<dyn TokenAuditLog>,
        queue: Arc<dyn TokenUsageQueue>,
        batch_size: usize,
        interval: Duration,
    ) -> Self {
        Self {
            usage: TokenUsageWorker::new(store.clone(), queue, batch_size),
            relay: TokenAuditRelay::new(store, sink),
            interval,
        }
    }

    pub(crate) async fn connect(
        store: Arc<dyn TokenStore>,
        queue: Arc<dyn TokenUsageQueue>,
        audit_queue_url: String,
        audit_timeout: Duration,
        aws_timeouts: Timeouts,
        batch_size: usize,
        interval: Duration,
    ) -> PlatformResult<Self> {
        let sdk_config = Aws::load_sdk_config(aws_timeouts).await;
        let sqs = Sqs::new(
            aws_sdk_sqs::Client::new(&sdk_config),
            aws_timeouts,
            SqsConfig::new(audit_queue_url),
        );
        let sink = Arc::new(SqsTokenAuditLog::new(Arc::new(sqs), audit_timeout));
        Ok(Self::new(store, sink, queue, batch_size, interval))
    }

    pub(crate) async fn drain(&self) -> PlatformResult<usize> {
        let applied = self.usage.drain().await;
        let delivered = self.relay.drain().await;
        match (applied, delivered) {
            (Ok(applied), Ok(delivered)) => Ok(applied + delivered),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    pub(crate) async fn run(self) -> PlatformResult<()> {
        let mut ticker = time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            match self.drain().await {
                Ok(count) => info!(count, "token drain cycle finished"),
                Err(error) => warn!(%error, "token drain cycle failed"),
            }
        }
    }
}
