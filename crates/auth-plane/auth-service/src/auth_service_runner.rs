use crate::{
    AuthService, AuthServiceConfig, CachedTokenStore, ValkeyReplayStore, ValkeyTokenUsageQueue,
    VaultTokenStore, jwt_server::JwtServer, jwt_server_config::JwtServerConfig,
};
use chrono::Duration;
use platform_errors::{PlatformError, PlatformResult};
use platform_runtime::{HealthRunner, Service};
use std::sync::Arc;
use tokio::net::TcpListener;

const SERVICE: Service = Service::new("platform-auth-service");

pub(crate) struct AuthServiceRunner;

impl AuthServiceRunner {
    pub(crate) async fn run(config: JwtServerConfig) -> PlatformResult<()> {
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
        let listener = TcpListener::bind(config.bind_address())
            .await
            .map_err(PlatformError::external)?;
        let server = JwtServer::new(listener, service, config.sigv4().clone());
        let health = HealthRunner::from_service_env("AUTH_SERVICE").await?;
        health.mark_ready();
        health.run(server.serve()).await
    }
}
