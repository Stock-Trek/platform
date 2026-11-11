use crate::config::AuthRelayConfig;
use aws_sdk_sqs::Client as SqsClient;
use platform_auth_service::{
    SqsTokenAuditLog, TokenAuditLog, TokenAuditRelay, TokenStore, TokenUsageQueue,
    TokenUsageWorker, ValkeyTokenUsageQueue, VaultTokenStore,
};
use platform_aws::Aws;
use platform_errors::BpResult;
use platform_sqs::sqs::{Sqs, SqsConfig};
use std::sync::Arc;

pub struct AuthRelay {
    relay: TokenAuditRelay,
    usage: Option<TokenUsageWorker>,
}

impl AuthRelay {
    pub async fn connect(config: &AuthRelayConfig) -> BpResult<Self> {
        let store = Arc::new(VaultTokenStore::new(config.vault.clone())?);
        let usage = Arc::new(ValkeyTokenUsageQueue::connect(config.usage_queue.clone()).await?);
        let sdk_config = Aws::load_sdk_config(config.aws_timeouts).await;
        let sqs = Sqs::new(
            SqsClient::new(&sdk_config),
            config.aws_timeouts,
            SqsConfig::new(config.audit_queue_url.clone()),
        );
        let sink = SqsTokenAuditLog::new(sqs, config.audit_timeout);
        Ok(Self::new(store, Arc::new(sink))
            .with_usage_queue(usage, config.usage_queue.batch_size()))
    }

    pub fn new(store: Arc<dyn TokenStore>, sink: Arc<dyn TokenAuditLog>) -> Self {
        Self {
            relay: TokenAuditRelay::new(store, sink),
            usage: None,
        }
    }

    pub fn with_usage_queue(mut self, queue: Arc<dyn TokenUsageQueue>, batch_size: usize) -> Self {
        self.usage = Some(TokenUsageWorker::new(self.relay_store(), queue, batch_size));
        self
    }

    fn relay_store(&self) -> Arc<dyn TokenStore> {
        self.relay.store()
    }

    pub async fn drain(&self) -> BpResult<usize> {
        let usage_result = match &self.usage {
            Some(worker) => worker.drain().await,
            None => Ok(0),
        };
        let audit_result = self.relay.drain().await;
        match (usage_result, audit_result) {
            (Ok(applied), Ok(delivered)) => Ok(applied + delivered),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
}
