mod aggregator;
mod config;
mod runner;
mod sink;
mod source;

use crate::{
    config::AggregatorConfig, runner::AggregatorRunner, sink::NatsCandleSink,
    source::NatsMarketEventSource,
};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = AggregatorConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    let source = NatsMarketEventSource::connect(&config).await?;
    let sink = NatsCandleSink::connect(&config).await?;
    AggregatorRunner::new(config, source, sink).run().await
}
