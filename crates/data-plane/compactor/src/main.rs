mod compactor;
mod config;
mod partition_output;
mod runner;
mod source;
mod spill;

use crate::{
    compactor::Compactor, config::CompactorConfig, runner::CompactorRunner, source::S3ArchiveSource,
};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = CompactorConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    let source = S3ArchiveSource::connect(&config).await?;
    let compactor = Compactor::connect(&config).await?;
    CompactorRunner::new(config, source, compactor).run().await
}
