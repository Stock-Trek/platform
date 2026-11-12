use platform_errors::BuildResult;
use platform_portfolio_service::{PortfolioConfig, PortfolioRunner};

#[tokio::main]
async fn main() -> BuildResult<()> {
    let config = PortfolioConfig::from_env()?;
    PortfolioRunner::run(config).await
}
