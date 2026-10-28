mod config;
mod runner;
mod sink;
mod source;

use crate::{
    config::ArchiverConfig, runner::ArchiverRunner, sink::S3ParquetSink, source::NatsArchiveSource,
};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = ArchiverConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    let source = NatsArchiveSource::connect(&config).await?;
    let sink = S3ParquetSink::connect(&config).await?;
    ArchiverRunner::new(config, source, sink).run().await
}
