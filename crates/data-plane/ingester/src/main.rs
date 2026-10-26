use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;
use platform_ingester::{IngesterConfig, run};

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = IngesterConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    run(config).await
}
