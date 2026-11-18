use platform_errors::PlatformResult;
use platform_front_end_api::{FrontEndApiConfig, FrontEndApiRunner};

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let config = FrontEndApiConfig::from_env()?;
    FrontEndApiRunner::run(config).await
}
