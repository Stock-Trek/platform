use crate::{config::PortfolioConfig, service::PortfolioService};
use platform_errors::BpResult;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-portfolio-service");

pub struct PortfolioRunner;

impl PortfolioRunner {
    pub async fn run(config: PortfolioConfig) -> BpResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let service = Arc::new(PortfolioService::in_memory());
        Self::wait_for_shutdown(&service).await
    }

    async fn wait_for_shutdown(_service: &PortfolioService) -> BpResult<()> {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}
