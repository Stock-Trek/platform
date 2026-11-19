use crate::{
    archiver_runner::ArchiverRunner, config::ArchiverConfig, sink::S3ParquetSink,
    source::NatsArchiveSource,
};
use platform_archive::IcebergCatalog;
use platform_errors::PlatformResult;
use platform_lease::NatsLeaseStore;
use platform_runtime::{HealthRunner, Service};
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-archiver");

pub struct ArchiverApp;

impl ArchiverApp {
    pub async fn run() -> PlatformResult<()> {
        let config = ArchiverConfig::from_env()?;
        SERVICE.init_logging(&config.log_filter)?;
        let _metrics = SERVICE.init_metrics("platform/archiver")?;
        let health = HealthRunner::from_service_env("ARCHIVER").await?;
        let source = NatsArchiveSource::connect(&config).await?;
        let catalog = Arc::new(IcebergCatalog::connect(&config.iceberg).await?);
        catalog.validate_contract().await?;
        let leases = Arc::new(
            NatsLeaseStore::connect(&config.nats_url, "platform-archiver", &config.lease_bucket)
                .await?,
        );
        let sink = S3ParquetSink::connect(&config, catalog, leases).await?;
        sink.recover_uncommitted().await?;
        health.mark_ready();
        health
            .run(ArchiverRunner::new(config, source, sink).run())
            .await
    }
}
