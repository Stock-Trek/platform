mod aggregator;
mod candle;
mod config;
mod nats;
mod runner;
mod sink;
mod source;

use crate::{config::AggregatorConfig, runner::AggregatorRunner};
use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = AggregatorConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    AggregatorRunner::new(config).run().await
}
