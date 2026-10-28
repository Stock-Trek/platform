mod backfiller;
mod binance;
mod config;
mod event_page_source;
mod fetcher;
mod gap_marker_source;
mod runner;

use crate::{
    binance::fetcher::BinanceGapFetcher, config::BackfillerConfig,
    gap_marker_source::NatsGapMarkerSource, runner::BackfillerRunner,
};
use platform_errors::BpResult;
use platform_nats::NatsPublisher;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-backfiller");
const NATS_CLIENT_NAME: &str = "platform-backfiller-events";

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = BackfillerConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let source = NatsGapMarkerSource::connect(&config).await?;
    let fetcher = BinanceGapFetcher::try_new(&config)?;
    let publisher = NatsPublisher::connect(&config.nats_url, NATS_CLIENT_NAME).await?;
    publisher
        .ensure_stream(&config.event_subject_prefix)
        .await?;
    BackfillerRunner::new(config, source, fetcher, publisher)
        .run()
        .await
}
