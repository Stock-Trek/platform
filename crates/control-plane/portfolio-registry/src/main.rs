use platform_errors::BuildResult;
use platform_portfolio_registry::{PortfolioRegistryConfig, RegistryRunner};

#[tokio::main]
async fn main() -> BuildResult<()> {
    let config = PortfolioRegistryConfig::from_env()?;
    RegistryRunner::run(config).await
}
