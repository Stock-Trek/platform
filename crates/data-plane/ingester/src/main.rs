mod config;
mod ingester_runner;
mod message_ingester;
mod sequence_tracker;
mod websocket_source;

#[cfg(test)]
mod ingester_runner_tests;
#[cfg(test)]
mod message_ingester_tests;
#[cfg(test)]
mod sequence_tracker_tests;
#[cfg(test)]
mod websocket_source_tests;

use crate::{
    config::IngesterConfig, ingester_runner::IngesterRunner, sequence_tracker::NatsSequenceStore,
    websocket_source::WebsocketSource,
};
use platform_errors::BuildResult;
use platform_nats::NatsPublisher;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-ingester");

#[tokio::main]
async fn main() -> BuildResult<()> {
    let config = IngesterConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/ingester")?;
    let publisher = Arc::new(NatsPublisher::connect(&config.nats_url, SERVICE.name()).await?);
    publisher.ensure_stream(&config.nats_subject_prefix).await?;
    let sequences =
        Arc::new(NatsSequenceStore::connect(&config.nats_url, &config.nats_subject_prefix).await?);
    let source = WebsocketSource::from_config(&config);
    IngesterRunner::new(config, source, publisher, sequences)
        .run()
        .await
}
