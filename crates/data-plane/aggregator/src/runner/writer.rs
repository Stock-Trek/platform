//! Fenced writer lease management for a shard processor.

use crate::{config::AggregatorConfig, shard_id::ShardId};
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::{Lease, LeaseStore};
use platform_metrics::AggregatorMetrics;
use platform_runtime::Runtime;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// The identity and active lease of the writer owning a shard.
pub struct Writer {
    id: String,
    lease: Lease,
}

impl Writer {
    /// Acquires the shard lease, or returns `None` if shutdown was
    /// requested while waiting for another writer to release it.
    pub async fn acquire(
        leases: &Arc<dyn LeaseStore>,
        shard: ShardId,
        requested_id: Option<String>,
        initial_lease: Option<Lease>,
        config: &AggregatorConfig,
    ) -> PlatformResult<Option<Self>> {
        let writer_id = requested_id
            .or_else(|| initial_lease.as_ref().map(|lease| lease.owner_id.clone()))
            .unwrap_or_else(Lease::new_owner_id);
        let lease = match initial_lease {
            Some(lease) => lease,
            None => {
                let request = shard.lease_request(&writer_id, config.lease_ttl);
                tokio::select! {
                    result = Lease::wait_for_lease(leases.as_ref(), &request) => result?,
                    signal = Runtime::shutdown_signal() => {
                        signal?;
                        return Ok(None);
                    }
                }
            }
        };

        AggregatorMetrics::record_rebalance(shard.get(), "lease_acquired");
        info!(
            shard = %shard,
            writer = %writer_id,
            fencing_token = lease.fencing_token,
            intervals = ?config
                .intervals
                .iter()
                .map(|interval| interval.label())
                .collect::<Vec<_>>(),
            "aggregator shard processor started"
        );

        Ok(Some(Self {
            id: writer_id,
            lease,
        }))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn lease(&self) -> &Lease {
        &self.lease
    }

    /// Extends the lease and replaces the local copy with the renewed one.
    pub async fn renew(
        &mut self,
        leases: &Arc<dyn LeaseStore>,
        shard: ShardId,
        ttl: Duration,
    ) -> PlatformResult<()> {
        let Some(renewed) = leases.renew(&self.lease, ttl).await? else {
            return Err(PlatformError::bad_value(
                "aggregator lease",
                shard.to_string(),
                format!("writer {} lost its fencing lease", self.id),
            ));
        };
        self.lease = renewed;
        Ok(())
    }

    pub async fn release(self, leases: &Arc<dyn LeaseStore>) -> PlatformResult<()> {
        leases.release(&self.lease).await.map(|_| ())
    }
}
