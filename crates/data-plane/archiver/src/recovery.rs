use crate::sink::S3ParquetSink;
use platform_archive::ArchiveIcebergTable;
use platform_errors::PlatformResult;
use platform_lease::{Lease, LeaseGuard, LeaseRequest, LeaseScope};

mod listing;
mod registration;

impl S3ParquetSink {
    fn recovery_scope(&self, table: ArchiveIcebergTable) -> LeaseScope {
        LeaseScope::new(format!(
            "archive-recovery:{}:{}:{}",
            self.bucket,
            self.prefix.trim_matches('/'),
            table.name()
        ))
    }

    pub(super) async fn acquire_recovery_guard(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<LeaseGuard> {
        let scope = self.recovery_scope(table);
        let owner_id = Lease::new_owner_id();
        let request = LeaseRequest::new(scope, &owner_id, self.lease_ttl)
            .with_initial_token(i64::try_from(self.lease_initial_token).unwrap_or(i64::MAX))?
            .with_clock_skew(self.lease_clock_skew)
            .with_context(table.name(), "");
        let lease = Lease::wait_for_lease(self.leases.as_ref(), &request).await?;
        LeaseGuard::adopt(self.leases.clone(), lease, request, self.max_work).await
    }
}

#[cfg(test)]
mod recovery_tests;
