use platform_errors::PlatformResult;
use platform_portfolio_service::{PortfolioConfig, PortfolioRunner};
use platform_runtime::HealthRunner;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let config = PortfolioConfig::from_env()?;
    let health = HealthRunner::from_service_env("PORTFOLIO_SERVICE").await?;
    let state = health.state().clone();
    health
        .run(PortfolioRunner::run_with_health(config, state))
        .await
}
