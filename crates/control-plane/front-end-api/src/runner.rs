use crate::api::{ApiServer, FrontEndApi, FrontEndApiDeps};
use crate::auth::SessionStore;
use crate::config::FrontEndApiConfig;
use chrono::Utc;
use platform_errors::PlatformResult;
use platform_runtime::Service;
use std::sync::Arc;
use std::time::Duration;

const SERVICE: Service = Service::new("platform-front-end-api");
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

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
        Self::spawn_retention_sweeper(Arc::clone(&api));
        let server = ApiServer::bind(config.bind_address(), api).await?;
        tracing::info!(address = %server.local_addr()?, "front-end api listening");
        server.serve().await
    }

    fn spawn_retention_sweeper(api: Arc<FrontEndApi>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RETENTION_SWEEP_INTERVAL);
            loop {
                interval.tick().await;
                let removed = api.apply_retention(Utc::now());
                if removed > 0 {
                    tracing::info!(removed, "retention removed expired runs");
                }
            }
        });
    }
}
