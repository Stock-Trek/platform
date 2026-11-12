mod compactor;
mod config;
mod content_hash;

#[cfg(test)]
mod content_hash_tests;

mod compactor_runner;
mod partition_output;

#[cfg(test)]
mod compactor_runner_tests;

mod source;
mod spill;

use crate::{
    compactor::Compactor, compactor_runner::CompactorRunner, config::CompactorConfig,
    source::S3ArchiveSource,
};
use platform_archive::IcebergCatalog;
use platform_errors::BuildResult;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-compactor");

#[tokio::main]
async fn main() -> BuildResult<()> {
    let config = CompactorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/compactor")?;
    let catalog = Arc::new(IcebergCatalog::connect(&config.iceberg).await?);
    let source = S3ArchiveSource::connect(&config, catalog.clone()).await?;
    let compactor = Compactor::connect(&config, catalog).await?;
    CompactorRunner::new(config, source, compactor).run().await
}
