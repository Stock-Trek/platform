use crate::config::AuthRelayConfig;
use aws_sdk_sqs::Client as SqsClient;
use platform_auth_service::{
    SqsTokenAuditLog, TokenAuditLog, TokenAuditRelay, TokenStore, VaultTokenStore,
};
use platform_aws::Aws;
use platform_errors::BpResult;
use platform_sqs::sqs::{Sqs, SqsConfig};
use std::sync::Arc;

pub struct AuthRelay {
    relay: TokenAuditRelay,
}

impl AuthRelay {
    pub async fn connect(config: &AuthRelayConfig) -> BpResult<Self> {
        let store = VaultTokenStore::new(config.vault.clone())?;
        let sdk_config = Aws::load_sdk_config(config.aws_timeouts).await;
        let sqs = Sqs::new(
            SqsClient::new(&sdk_config),
            config.aws_timeouts,
            SqsConfig::new(config.audit_queue_url.clone()),
        );
        let sink = SqsTokenAuditLog::new(sqs, config.audit_timeout);
        Ok(Self::new(Arc::new(store), Arc::new(sink)))
    }

    pub fn new(store: Arc<dyn TokenStore>, sink: Arc<dyn TokenAuditLog>) -> Self {
        Self {
            relay: TokenAuditRelay::new(store, sink),
        }
    }

    pub async fn drain(&self) -> BpResult<usize> {
        self.relay.drain().await
    }
}
