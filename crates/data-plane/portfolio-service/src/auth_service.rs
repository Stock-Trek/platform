use crate::config::AuthConfig;
use aws_sdk_sqs::Client as SqsClient;
use chrono::Duration;
use platform_auth_service::{
    AuthService, AuthServiceConfig, SqsTokenAuditLog, ValkeyReplayStore, VaultTokenStore,
};
use platform_aws::Aws;
use platform_errors::BpResult;
use platform_sqs::sqs::{Sqs, SqsConfig};
use platform_time::timeouts::Timeouts;
use std::sync::Arc;

pub struct PortfolioAuthService;

impl PortfolioAuthService {
    pub async fn connect(config: AuthConfig) -> BpResult<AuthService> {
        let store = VaultTokenStore::new(config.vault().clone())?;
        let timeouts = Timeouts::from_durations(config.audit().timeout(), config.audit().timeout());
        let sdk_config = Aws::load_sdk_config(timeouts).await;
        let sqs = Sqs::new(
            SqsClient::new(&sdk_config),
            timeouts,
            SqsConfig::new(config.audit().queue_url().to_string()),
        );
        let audit = SqsTokenAuditLog::new(sqs, config.audit().timeout());
        let replay = ValkeyReplayStore::connect(config.valkey().clone()).await?;
        AuthService::new(
            Arc::new(store),
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
                audit: Arc::new(audit),
            },
            Arc::new(replay),
        )
    }
}
