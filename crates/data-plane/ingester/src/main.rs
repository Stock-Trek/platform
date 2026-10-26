use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;
use platform_ingester::{IngesterConfig, IngesterRunner};

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = IngesterConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    IngesterRunner::new(config).run().await
}
