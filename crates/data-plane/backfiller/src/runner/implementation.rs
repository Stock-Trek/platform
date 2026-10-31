use crate::{
    backfiller::{BackfillOptions, BackfillService},
    config::BackfillerConfig,
    fetcher::MissingEventFetcher,
    gap_marker_source::GapMarkerSource,
    retry::GapRetryStore,
};
use platform_errors::BpResult;
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

    pub async fn run_with_retry_store<R>(self, retries: R) -> BpResult<()>
    where
        R: GapRetryStore + 'static,
    {
        self.run_inner(Arc::new(retries)).await
    }

    async fn run_inner(self, retries: Arc<dyn GapRetryStore>) -> BpResult<()> {
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
