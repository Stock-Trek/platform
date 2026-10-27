mod config;
mod ingester;
mod nats;
mod publisher;
mod runner;
mod sequence;
mod source;

use crate::{config::IngesterConfig, runner::IngesterRunner};
use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = IngesterConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    IngesterRunner::new(config).run().await
}
