mod compactor;
mod config;
mod content_id;
mod partition_output;
mod runner;
mod source;
mod spill;

use crate::{
    compactor::Compactor, config::CompactorConfig, runner::CompactorRunner, source::S3ArchiveSource,
};
use platform_archive::IcebergCatalog;
use platform_errors::BpResult;
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-compactor");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = CompactorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let catalog = Arc::new(IcebergCatalog::connect(&config.iceberg).await?);
    let source = S3ArchiveSource::connect(&config, catalog.clone()).await?;
    let compactor = Compactor::connect(&config, catalog).await?;
    CompactorRunner::new(config, source, compactor).run().await
}
