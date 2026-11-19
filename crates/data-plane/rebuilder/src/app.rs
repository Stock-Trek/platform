use crate::RebuildConfig;
use platform_errors::{PlatformError, PlatformResult};
use platform_runtime::{HealthRunner, Runtime, Service};

const SERVICE: Service = Service::new("platform-rebuilder");

pub struct RebuilderApp;

impl RebuilderApp {
    pub async fn run() -> PlatformResult<()> {
        let config = RebuildConfig::from_env()?;
        SERVICE.init_logging(&config.log_filter)?;
        let _metrics = SERVICE.init_metrics("platform/rebuilder")?;
        let health = HealthRunner::from_service_env("REBUILDER").await?;
        health.mark_ready();
        health
            .run(async {
                tokio::select! {
                    result = config.run() => result,
                    signal = Runtime::shutdown_signal() => {
                        signal?;
                        Err(PlatformError::external(
                            "rebuild interrupted by shutdown before completion",
                        ))
                    }
                }
            })
            .await
    }
}
