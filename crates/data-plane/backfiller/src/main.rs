mod backfiller;
mod binance;
mod config;
mod event_page_source;
mod fetcher;
mod gap_marker_source;
mod runner;

use crate::{config::BackfillerConfig, runner::BackfillerRunner};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = BackfillerConfig::from_env()?;
    platform_runtime::init_logging(&config.log_filter)?;
    BackfillerRunner::new(config).run().await
}
