mod backfiller;
mod binance;
mod config;
mod event_page_source;
mod fetcher;
mod gap_marker_source;
mod retry;
mod runner;

use crate::{
    binance::{EXCHANGE_API_KEY_SECRET, fetcher::BinanceGapFetcher},
    config::BackfillerConfig,
    fetcher::FetcherRegistry,
    gap_marker_source::NatsGapMarkerSource,
    retry::NatsGapRetryStore,
    runner::BackfillerRunner,
};
use platform_aws::{Aws, secrets::Secrets};
use platform_errors::BpResult;
use platform_nats::NatsPublisher;
use platform_runtime::Service;
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use std::time::{Duration, SystemTime};

const SERVICE: Service = Service::new("platform-backfiller");
const NATS_CLIENT_NAME: &str = "platform-backfiller-events";
const AWS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const AWS_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const SECRETS_LOAD_DEADLINE: Duration = Duration::from_secs(30);
const SECRETS_LOAD_BUFFER: Duration = Duration::from_secs(5);

async fn exchange_api_key(config: &BackfillerConfig) -> BpResult<String> {
    let timeouts = Timeouts::from_durations(AWS_CONNECT_TIMEOUT, AWS_OPERATION_TIMEOUT);
    let sdk_config = Aws::load_sdk_config(timeouts).await;
    let deadline = Deadline::new(
        SystemTime::now() + SECRETS_LOAD_DEADLINE,
        SECRETS_LOAD_BUFFER,
    );
    let secrets = Secrets::load(&sdk_config, timeouts, deadline, &config.ssm).await?;
    secrets.get(EXCHANGE_API_KEY_SECRET)
}

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = BackfillerConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let api_key = exchange_api_key(&config).await?;
    let source = NatsGapMarkerSource::connect(&config).await?;
    let mut fetchers = FetcherRegistry::new();
    fetchers.register(
        config.exchange,
        BinanceGapFetcher::try_new(&config, api_key)?,
    );
    let retries = NatsGapRetryStore::connect(&config).await?;
    let publisher = NatsPublisher::connect(&config.nats_url, NATS_CLIENT_NAME).await?;
    publisher
        .ensure_stream(&config.event_subject_prefix)
        .await?;
    BackfillerRunner::new(config, source, fetchers, publisher)
        .run_with_retry_store(retries)
        .await
}
