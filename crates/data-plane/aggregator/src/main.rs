mod aggregator;
mod config;
mod runner;
mod sink;
mod source;

use crate::{config::AggregatorConfig, runner::AggregatorRunner};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = AggregatorConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    AggregatorRunner::new(config).run().await
}
