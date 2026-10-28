mod backfiller;
mod binance;
mod config;
mod event_page_source;
mod fetcher;
mod gap_marker_source;
mod publisher;
mod runner;

use crate::{
    binance::fetcher::BinanceGapFetcher, config::BackfillerConfig,
    gap_marker_source::NatsGapMarkerSource, runner::BackfillerRunner,
};
use platform_errors::BpResult;
use platform_nats::NatsPublisher;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = BackfillerConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    let source = NatsGapMarkerSource::connect(&config).await?;
    let fetcher = BinanceGapFetcher::try_new(&config)?;
    let publisher = NatsPublisher::connect(&config.nats_url, "platform-backfiller-events").await?;
    BackfillerRunner::new(config, source, fetcher, publisher)
        .run()
        .await
}
