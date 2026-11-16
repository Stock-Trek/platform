mod compactor;
mod compactor_runner;
mod config;
mod partition_output;
mod source;
mod source_inventory;
mod spill;

#[cfg(test)]
mod config_tests;

#[cfg(test)]
mod compactor_runner_tests;

use crate::{
    compactor::Compactor, compactor_runner::CompactorRunner, config::CompactorConfig,
    source::S3ArchiveSource,
};
use platform_archive::IcebergCatalog;
use platform_errors::PlatformResult;
use platform_lease::NatsLeaseStore;
use platform_runtime::{HealthRunner, Service};
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-compactor");

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let config = CompactorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/compactor")?;
    let health = HealthRunner::from_service_env("COMPACTOR").await?;
    let catalog = Arc::new(IcebergCatalog::connect(&config.iceberg).await?);
    let source = S3ArchiveSource::connect(&config, catalog.clone()).await?;
    let leases = Arc::new(
        NatsLeaseStore::connect(&config.nats_url, "platform-compactor", &config.lease_bucket)
            .await?,
    );
    let compactor = Compactor::connect(&config, catalog, leases).await?;
    health.mark_ready();
    health
        .run(CompactorRunner::new(config, source, compactor).run())
        .await
}
