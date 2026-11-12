mod backfiller;
mod binance;
mod config;
mod event_page_source;
mod fetcher;
mod gap_marker_source;
mod retry;
mod runner;

use crate::{
    binance::fetcher::BinanceGapFetcher, config::BackfillerConfig, fetcher::FetcherRegistry,
    gap_marker_source::NatsGapMarkerSource, retry::NatsGapRetryStore, runner::BackfillerRunner,
};
use platform_errors::BuildResult;
use platform_nats::NatsPublisher;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-backfiller");
const NATS_CLIENT_NAME: &str = "platform-backfiller-events";
#[tokio::main]
async fn main() -> BuildResult<()> {
    let config = BackfillerConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/backfiller")?;
    let credentials = config.exchange_credentials().await?;
    let source = NatsGapMarkerSource::connect(&config).await?;
    let mut fetchers = FetcherRegistry::default();
    fetchers.insert(
        config.exchange,
        BinanceGapFetcher::try_new(&config, credentials)?,
    );
    let retries = NatsGapRetryStore::connect(&config).await?;
    let publisher = NatsPublisher::connect(&config.nats_url, NATS_CLIENT_NAME).await?;
    publisher
        .ensure_stream(&config.event_subject_prefix)
        .await?;
    BackfillerRunner::new(config, source, fetchers, publisher)
        .run(retries)
        .await
}
