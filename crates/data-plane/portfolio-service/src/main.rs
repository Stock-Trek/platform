use platform_errors::BpResult;
use platform_portfolio_service::{PortfolioConfig, PortfolioRunner};

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = PortfolioConfig::from_env()?;
    PortfolioRunner::run(config).await
}
