use crate::config::MaintainerConfig;
use crate::lifecycle::LifecycleBackstop;
use platform_archive::{
    ArchiveCatalog, ArchiveMaintenanceExecutor, IcebergCatalog, MaintenanceRunner, ObjectStore, S3,
    S3ObjectStore,
};
use platform_errors::PlatformResult;
use platform_lease::NatsLeaseStore;
use platform_runtime::Runtime;
use std::sync::Arc;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

/// Dedicated archive maintenance service.
///
/// Owns the four-stage maintenance runner in production: snapshot expiry,
/// orphan cleanup, compacted and ledger rewriting, and raw-table cleanup with
/// raw object deletion, on a fixed interval with an optional dry-run mode.
pub(crate) struct MaintainerRunner {
    config: MaintainerConfig,
    catalog: Arc<dyn ArchiveCatalog>,
    objects: Arc<dyn ObjectStore>,
    leases: Arc<dyn platform_lease::LeaseStore>,
}

impl MaintainerRunner {
    pub(crate) async fn connect(config: MaintainerConfig) -> PlatformResult<Self> {
        let catalog = Arc::new(IcebergCatalog::connect(&config.iceberg).await?);
        catalog.validate_contract().await?;
        let client = S3::s3_client().await;
        let objects = Arc::new(S3ObjectStore::new(client.clone()));
        let leases = Arc::new(
            NatsLeaseStore::connect(
                &config.nats_url,
                "platform-maintainer",
                &config.lease_bucket,
            )
            .await?,
        );
        Ok(Self {
            config,
            catalog,
            objects,
            leases,
        })
    }

    /// Installs the S3 lifecycle backstop policy once at startup.
    pub(crate) async fn apply_lifecycle_backstop(&self) -> PlatformResult<()> {
        if !self.config.s3_lifecycle_enabled {
            info!("S3 lifecycle backstop disabled by configuration");
            return Ok(());
        }
        LifecycleBackstop::new(S3::s3_client().await, self.config.s3_bucket.clone())
            .apply()
            .await
    }

    pub(crate) async fn run(self) -> PlatformResult<()> {
        let mut ticker = time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        info!(
            s3_bucket = %self.config.s3_bucket,
            output_prefix = %self.config.output_prefix,
            interval_secs = self.config.interval.as_secs(),
            dry_run = self.config.dry_run,
            "archive maintainer started"
        );
        Runtime::run_until_shutdown(async move {
            loop {
                ticker.tick().await;
                if let Err(error) = self.run_maintenance().await {
                    warn!(%error, "archive maintenance run failed");
                }
            }
        })
        .await
    }

    pub(crate) async fn run_maintenance(&self) -> PlatformResult<()> {
        let executor = ArchiveMaintenanceExecutor::new(
            self.catalog.clone(),
            self.objects.clone(),
            self.leases.clone(),
            self.config.maintenance.clone(),
            self.config.s3_bucket.clone(),
            self.config.output_prefix.clone(),
        )?;
        let runner = MaintenanceRunner::new(
            self.config.maintenance.clone(),
            executor,
            self.leases.clone(),
        )?;
        let report = runner.run(self.config.dry_run).await?;
        info!(
            dry_run = report.dry_run,
            lease_acquired = report.acquired,
            completed = report.completed,
            stages = ?report.stages,
            "archive maintenance run finished"
        );
        Ok(())
    }
}
