use crate::sink::S3ParquetSink;
use platform_archive::{
    ArchiveIcebergTable, ArchiveRegistration, CommitContext, IcebergDataFile, RegisterOutcome,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::LeaseGuard;
use platform_metrics::MarketDataMetrics;
use tracing::{info, warn};

const RECOVERY_MAX_ATTEMPTS: usize = 4;

impl S3ParquetSink {
    pub(super) async fn register_pending_pages(
        &self,
        table: ArchiveIcebergTable,
        pending: Vec<IcebergDataFile>,
        guard: &mut LeaseGuard,
    ) -> PlatformResult<()> {
        let mut page = Vec::new();
        let mut page_bytes = 0_usize;
        for file in pending {
            let file_bytes = usize::try_from(file.file_size_in_bytes).unwrap_or(usize::MAX);
            if !page.is_empty() && page_bytes.saturating_add(file_bytes) > self.recovery_page_bytes
            {
                self.register_pending(table, std::mem::take(&mut page), guard)
                    .await?;
                page_bytes = 0;
            }
            page_bytes = page_bytes.saturating_add(file_bytes);
            page.push(file);
        }
        self.register_pending(table, page, guard).await
    }

    pub(super) async fn register_pending(
        &self,
        table: ArchiveIcebergTable,
        mut pending: Vec<IcebergDataFile>,
        guard: &mut LeaseGuard,
    ) -> PlatformResult<()> {
        for attempt in 1..=RECOVERY_MAX_ATTEMPTS {
            if pending.is_empty() {
                return Ok(());
            }
            guard.ensure_writable().await?;
            let context = CommitContext::default()
                .with_lease(
                    guard.lease().scope.as_str(),
                    &guard.lease().owner_id,
                    guard.lease().fencing_token,
                )
                .with_authorizer(guard.authorizer());
            let registration = ArchiveRegistration::new(pending.clone(), None::<Vec<String>>)?;
            let digest = registration.batch_digest();
            match self
                .catalog
                .register_files(table, &registration, &context)
                .await?
            {
                RegisterOutcome::Applied => {
                    MarketDataMetrics::record_archive_recovery(
                        table.name(),
                        "registered",
                        pending.len() as u64,
                    );
                    info!(
                        table = table.name(),
                        files = pending.len(),
                        "recovered uncommitted archive files"
                    );
                    return Ok(());
                }
                RegisterOutcome::AlreadyApplied => {
                    MarketDataMetrics::record_archive_recovery(
                        table.name(),
                        "already_live",
                        pending.len() as u64,
                    );
                    info!(
                        table = table.name(),
                        files = pending.len(),
                        "recovered uncommitted archive files"
                    );
                    return Ok(());
                }
                RegisterOutcome::Conflict(conflict) => {
                    warn!(
                        table = table.name(),
                        attempt,
                        scope = %guard.lease().scope.as_str(),
                        owner = %guard.lease().owner_id,
                        token = guard.lease().fencing_token,
                        source_batch_digest = %digest,
                        claimed_sources = ?conflict.claimed_sources,
                        live_outputs = ?conflict.live_outputs,
                        "archive recovery conflict; reloading live files"
                    );
                    let live = self.catalog.registered_data_files(table).await?;
                    pending.retain(|file| !live.contains(&file.file_path));
                    MarketDataMetrics::record_archive_recovery(
                        table.name(),
                        "deferred_by_conflict",
                        pending.len() as u64,
                    );
                }
            }
        }
        if pending.is_empty() {
            Ok(())
        } else {
            MarketDataMetrics::record_archive_recovery(
                table.name(),
                "unresolved",
                pending.len() as u64,
            );
            Err(PlatformError::external(std::io::Error::other(format!(
                "archive recovery left {} unregistered files",
                pending.len()
            ))))
        }
    }
}
