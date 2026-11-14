use platform_errors::PlatformResult;
use platform_portfolio_service::{PortfolioConfig, PortfolioRunner};

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let config = PortfolioConfig::from_env()?;
    PortfolioRunner::run(config).await
}
