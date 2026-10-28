mod compactor;
mod config;
mod partition_output;
mod runner;
mod source;
mod spill;

use crate::{config::CompactorConfig, runner::CompactorRunner};
use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = CompactorConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    CompactorRunner::new(config).run().await
}
