mod config;
mod ingester;
mod runner;
mod sequence;
mod source;

use crate::{
    config::IngesterConfig, runner::IngesterRunner, sequence::NatsSequenceStore,
    source::WebsocketSource,
};
use platform_errors::BpResult;
use platform_nats::NatsPublisher;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-ingester");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = IngesterConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let publisher = Arc::new(NatsPublisher::connect(&config.nats_url, SERVICE.name()).await?);
    publisher.ensure_stream(&config.nats_subject_prefix).await?;
    let sequences =
        Arc::new(NatsSequenceStore::connect(&config.nats_url, &config.nats_subject_prefix).await?);
    let source = WebsocketSource::from_config(&config);
    IngesterRunner::new(config, source, publisher)
        .with_sequence_store(sequences)
        .run()
        .await
}
