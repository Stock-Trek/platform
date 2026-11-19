use crate::{config::MaintainerConfig, runner::MaintainerRunner};
use platform_errors::PlatformResult;
use platform_runtime::{HealthRunner, Service};

const SERVICE: Service = Service::new("platform-maintainer");

pub struct MaintainerApp;

impl MaintainerApp {
    pub async fn run() -> PlatformResult<()> {
        let config = MaintainerConfig::from_env()?;
        SERVICE.init_logging(&config.log_filter)?;
        let _metrics = SERVICE.init_metrics("platform/maintainer")?;
        let health = HealthRunner::from_service_env("MAINTAINER").await?;
        let runner = MaintainerRunner::connect(config).await?;
        runner.apply_lifecycle_backstop().await?;
        health.mark_ready();
        health.run(runner.run()).await
    }
}
