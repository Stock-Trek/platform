mod config;
mod ingester;
mod publisher;
mod runner;
mod sequence;
mod source;

use crate::{config::IngesterConfig, runner::IngesterRunner, source::WebsocketSource};
use platform_errors::BpResult;
use platform_nats::NatsPublisher;
use std::sync::Arc;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = IngesterConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    let publisher = Arc::new(NatsPublisher::connect(&config.nats_url, "platform-ingester").await?);
    let source = WebsocketSource::from_config(&config);
    IngesterRunner::new(config, source, publisher).run().await
}
