use crate::api::{ApiServer, FrontEndApi, FrontEndApiDeps};
use crate::auth::SessionStore;
use crate::config::FrontEndApiConfig;
use platform_errors::PlatformResult;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-front-end-api");

pub struct FrontEndApiRunner;

impl FrontEndApiRunner {
    pub async fn run(config: FrontEndApiConfig) -> PlatformResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let _metrics = SERVICE.init_metrics("platform/front-end-api")?;
        let store = Arc::new(SessionStore::new());
        let api = Arc::new(FrontEndApi::new(FrontEndApiDeps {
            session_store: store,
            allowed_origins: config.allowed_origins().to_vec(),
        }));
        let server = ApiServer::bind(config.bind_address(), api).await?;
        tracing::info!(address = %server.local_addr()?, "front-end api listening");
        server.serve().await
    }
}
