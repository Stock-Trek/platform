use crate::{
    backfiller_runner::BackfillerRunner, binance::fetcher::BinanceGapFetcher,
    config::BackfillerConfig, event_fetcher::FetcherRegistry, gap_retry_store::NatsGapRetryStore,
    nats_gap_marker_source::NatsGapMarkerSource,
};
use platform_errors::PlatformResult;
use platform_nats::NatsPublisher;
use platform_runtime::{HealthRunner, Service};

const SERVICE: Service = Service::new("platform-backfiller");
const NATS_CLIENT_NAME: &str = "platform-backfiller-events";

pub struct BackfillerApp;

impl BackfillerApp {
    pub async fn run() -> PlatformResult<()> {
        let config = BackfillerConfig::from_env()?;
        SERVICE.init_logging(&config.log_filter)?;
        let _metrics = SERVICE.init_metrics("platform/backfiller")?;
        let health = HealthRunner::from_service_env("BACKFILLER").await?;
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
        health.mark_ready();
        health
            .run(BackfillerRunner::new(config, source, fetchers, publisher).run(retries))
            .await
    }
}
