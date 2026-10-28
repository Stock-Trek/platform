mod backfiller;
mod binance;
mod config;
mod fetcher;
mod runner;
mod source;

use crate::{config::BackfillerConfig, runner::BackfillerRunner};
use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = BackfillerConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    BackfillerRunner::new(config).run().await
}
