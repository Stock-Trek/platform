use crate::config::AuthConfig;
use chrono::Duration;
use platform_auth_service::{AuthService, AuthServiceConfig, ValkeyReplayStore, VaultTokenStore};
use platform_errors::BpResult;
use std::sync::Arc;

pub struct PortfolioAuthService;

impl PortfolioAuthService {
    pub async fn connect(config: AuthConfig) -> BpResult<AuthService> {
        let store = VaultTokenStore::new(config.vault().clone())?;
        let replay = ValkeyReplayStore::connect(config.valkey().clone()).await?;
        AuthService::new(
            Arc::new(store),
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
            },
            Arc::new(replay),
        )
    }
}
