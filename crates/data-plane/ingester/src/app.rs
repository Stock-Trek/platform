use crate::{
    config::IngesterConfig, ingester_runner::IngesterRunner, sequence_tracker::NatsSequenceStore,
    websocket_source::WebsocketSource,
};
use platform_errors::PlatformResult;
use platform_nats::NatsPublisher;
use platform_runtime::{HealthRunner, Service};
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-ingester");

pub struct IngesterApp;

impl IngesterApp {
    pub async fn run() -> PlatformResult<()> {
        let config = IngesterConfig::from_env()?;
        SERVICE.init_logging(&config.log_filter)?;
        let _metrics = SERVICE.init_metrics("platform/ingester")?;
        let health = HealthRunner::from_service_env("INGESTER").await?;
        let publisher = Arc::new(NatsPublisher::connect(&config.nats_url, SERVICE.name()).await?);
        publisher.ensure_stream(&config.nats_subject_prefix).await?;
        let sequences = Arc::new(
            NatsSequenceStore::connect(&config.nats_url, &config.nats_subject_prefix).await?,
        );
        let source = WebsocketSource::from_config(&config);
        health.mark_ready();
        health
            .run(IngesterRunner::new(config, source, publisher, sequences).run())
            .await
    }
}
