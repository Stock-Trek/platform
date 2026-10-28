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
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-aggregator");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = AggregatorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let source = NatsMarketEventSource::connect(&config).await?;
    let sink = NatsCandleSink::connect(&config).await?;
    AggregatorRunner::new(config, source, sink).run().await
}
