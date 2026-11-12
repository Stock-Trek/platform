use crate::{
    AuthService, AuthServiceConfig, CachedTokenStore, JwtServer, JwtServerConfig,
    ValkeyReplayStore, ValkeyTokenUsageQueue, VaultTokenStore,
};
use chrono::Duration;
use platform_errors::BpResult;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-auth-service");

pub struct AuthServiceRunner;

impl AuthServiceRunner {
    pub async fn run(config: JwtServerConfig) -> BpResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let _metrics = SERVICE.init_metrics("platform/auth-service")?;
        let vault = Arc::new(VaultTokenStore::new(config.vault().clone())?);
        let store = Arc::new(CachedTokenStore::new(vault, config.token_cache().clone()));
        let replay = Arc::new(ValkeyReplayStore::connect(config.valkey().clone()).await?);
        let usage = Arc::new(ValkeyTokenUsageQueue::connect(config.usage_queue().clone()).await?);
        let service = AuthService::with_jwt_signing_key(
            store,
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
            },
            replay,
            Arc::new(config.jwt_signing_key()?),
            usage,
        )?;
        let server =
            JwtServer::bind_with_sigv4(config.bind_address(), service, config.sigv4().clone())
                .await?;
        server.serve().await
    }
}
