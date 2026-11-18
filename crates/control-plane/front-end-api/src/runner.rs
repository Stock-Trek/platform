use crate::api::{ApiServer, FrontEndApi};
use crate::auth::SessionService;
use crate::auth::SessionStore;
use crate::config::FrontEndApiConfig;
use platform_auth_service::JwksCache;
use platform_errors::PlatformResult;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-front-end-api");

pub struct FrontEndApiRunner;

impl FrontEndApiRunner {
    pub async fn run(config: FrontEndApiConfig) -> PlatformResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let _metrics = SERVICE.init_metrics("platform/front-end-api")?;
        // TODO: mint and verify real EdDSA tokens against the auth plane instead of the demo session store.
        let store = Arc::new(SessionStore::new());
        let _jwks = JwksCache::new(config.jwks_url())?;
        let sessions = Arc::new(SessionService::new(store));
        let api = Arc::new(FrontEndApi::new(sessions));
        let server = ApiServer::bind(config.bind_address(), api).await?;
        server.serve().await
    }
}
