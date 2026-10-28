mod compactor;
mod config;
mod parquet;
mod record;
mod runner;
mod source;

use crate::{config::CompactorConfig, runner::CompactorRunner};
use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = CompactorConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    CompactorRunner::new(config).run().await
}
