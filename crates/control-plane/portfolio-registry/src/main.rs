use platform_errors::PlatformResult;
use platform_portfolio_registry::{PortfolioRegistryConfig, RegistryRunner};

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let config = PortfolioRegistryConfig::from_env()?;
    RegistryRunner::run(config).await
}
