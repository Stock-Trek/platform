mod buffer;
mod config;
mod parquet;
mod record;
mod runner;
mod sink;
mod source;

use crate::{config::ArchiverConfig, runner::ArchiverRunner};
use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = ArchiverConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    ArchiverRunner::new(config).run().await
}
