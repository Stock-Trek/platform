use crate::{
    backfiller::{BackfillOptions, BackfillService},
    binance::fetcher::BinanceGapFetcher,
    config::BackfillerConfig,
    gap_marker_source::NatsGapMarkerSource,
};
use platform_errors::BpResult;
use platform_nats::NatsPublisher;
use tracing::info;

pub struct BackfillerRunner {
    config: BackfillerConfig,
}

impl BackfillerRunner {
    pub fn new(config: BackfillerConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> BpResult<()> {
        let source = NatsGapMarkerSource::connect(&self.config).await?;
        let fetcher = BinanceGapFetcher::try_new(&self.config)?;
        let publisher =
            NatsPublisher::connect(&self.config.nats_url, "platform-backfiller-events").await?;
        let options = BackfillOptions {
            exchange: self.config.exchange,
            event_subject_prefix: self.config.event_subject_prefix.clone(),
        };

        info!(
            exchange = %self.config.exchange,
            gap_subject_prefix = %self.config.gap_subject_prefix,
            event_subject_prefix = %self.config.event_subject_prefix,
            "backfiller started"
        );
        BackfillService::new(source, fetcher, publisher, options)
            .run()
            .await
    }
}
