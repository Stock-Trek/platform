use crate::config::AuthConfig;
use chrono::Duration;
use platform_auth_service::{
    AuthService, AuthServiceConfig, CachedTokenStore, ValkeyReplayStore, ValkeyTokenUsageQueue,
    VaultTokenStore,
};
use platform_errors::BuildResult;
use std::sync::Arc;

pub struct PortfolioAuthService;

impl PortfolioAuthService {
    pub async fn connect(config: AuthConfig) -> BuildResult<AuthService> {
        let vault = Arc::new(VaultTokenStore::new(config.vault().clone())?);
        let store = Arc::new(CachedTokenStore::new(vault, config.token_cache().clone()));
        let replay = ValkeyReplayStore::connect(config.valkey().clone()).await?;
        let usage = Arc::new(ValkeyTokenUsageQueue::connect(config.usage_queue().clone()).await?);
        AuthService::new(
            store,
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
            },
            Arc::new(replay),
            usage,
        )
    }
}
