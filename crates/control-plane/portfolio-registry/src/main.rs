use platform_errors::BpResult;
use platform_portfolio_registry::{PortfolioRegistryConfig, RegistryRunner};

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = PortfolioRegistryConfig::from_env()?;
    RegistryRunner::run(config).await
}
