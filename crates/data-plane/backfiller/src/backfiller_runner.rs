use crate::{
    backfiller::{BackfillOptions, BackfillService},
    config::BackfillerConfig,
    event_fetcher::MissingEventFetcher,
    gap_retry_store::GapRetryStore,
    nats_gap_marker_source::GapMarkerSource,
};
use platform_errors::BuildResult;
use platform_nats::EventPublisher;
use std::sync::Arc;
use tracing::info;

pub struct BackfillerRunner<S, F, P> {
    config: BackfillerConfig,
    source: S,
    fetcher: F,
    publisher: P,
}

impl<S, F, P> BackfillerRunner<S, F, P>
where
    S: GapMarkerSource,
    F: MissingEventFetcher,
    P: EventPublisher,
{
    pub fn new(config: BackfillerConfig, source: S, fetcher: F, publisher: P) -> Self {
        Self {
            config,
            source,
            fetcher,
            publisher,
        }
    }

    pub async fn run<R>(self, retries: R) -> BuildResult<()>
    where
        R: GapRetryStore + 'static,
    {
        let retries = Arc::new(retries);
        let Self {
            config,
            source,
            fetcher,
            publisher,
        } = self;
        let options = BackfillOptions {
            event_subject_prefix: config.event_subject_prefix.clone(),
        };

        info!(
            exchange = %config.exchange,
            gap_subject_prefix = %config.gap_subject_prefix,
            event_subject_prefix = %config.event_subject_prefix,
            "backfiller started"
        );
        BackfillService::new(source, fetcher, publisher, options)
            .with_retry_store_arc(retries)
            .run()
            .await
    }
}
