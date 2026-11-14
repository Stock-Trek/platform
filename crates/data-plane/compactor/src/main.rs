mod compactor;
mod compactor_runner;
mod config;
mod content_hash;
mod partition_output;
mod source;
mod spill;

#[cfg(test)]
mod compactor_runner_tests;
#[cfg(test)]
mod content_hash_tests;

use crate::{
    compactor::Compactor, compactor_runner::CompactorRunner, config::CompactorConfig,
    source::S3ArchiveSource,
};
use platform_archive::IcebergCatalog;
use platform_errors::PlatformResult;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-compactor");

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let config = CompactorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/compactor")?;
    let catalog = Arc::new(IcebergCatalog::connect(&config.iceberg).await?);
    let source = S3ArchiveSource::connect(&config, catalog.clone()).await?;
    let compactor = Compactor::connect(&config, catalog).await?;
    CompactorRunner::new(config, source, compactor).run().await
}
