mod config;
mod runner;
mod sink;
mod source;

use crate::{
    config::ArchiverConfig, runner::ArchiverRunner, sink::S3ParquetSink, source::NatsArchiveSource,
};
use platform_errors::BpResult;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-archiver");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = ArchiverConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let source = NatsArchiveSource::connect(&config).await?;
    let sink = S3ParquetSink::connect(&config).await?;
    ArchiverRunner::new(config, source, sink).run().await
}
