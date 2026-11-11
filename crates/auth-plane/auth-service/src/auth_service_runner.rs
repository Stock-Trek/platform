use crate::{
    AuthService, AuthServiceConfig, JwtServer, JwtServerConfig, MemoryReplayStore, VaultTokenStore,
};
use chrono::Duration;
use platform_errors::{BpError, BpResult};
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-auth-service");

pub struct AuthServiceRunner;

impl AuthServiceRunner {
    pub async fn run(config: JwtServerConfig) -> BpResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let store = Arc::new(VaultTokenStore::new(config.vault().clone())?);
        let service = AuthService::with_jwt_signing_key(
            store,
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
            },
            Arc::new(MemoryReplayStore::default()),
            Arc::new(config.jwt_signing_key()?),
        )?;
        let server =
            JwtServer::bind_with_sigv4(config.bind_address(), service, config.sigv4().clone())
                .await?;
        tokio::select! {
            result = server.serve() => result,
            result = tokio::signal::ctrl_c() => result.map_err(BpError::external),
        }
    }
}
