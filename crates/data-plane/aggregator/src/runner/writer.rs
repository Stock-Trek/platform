//! Fenced writer lease management for a shard processor.

use crate::{
    config::AggregatorConfig,
    lease::{LeaseStore, WriterLease},
    shard::ShardId,
};
use platform_errors::{BpError, BpResult};
use platform_metrics::AggregatorMetrics;
use platform_runtime::Runtime;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// The identity and active lease of the writer owning a shard.
pub(super) struct Writer {
    id: String,
    lease: WriterLease,
}

impl Writer {
    /// Acquires the shard lease, or returns `None` if shutdown was
    /// requested while waiting for another writer to release it.
    pub(super) async fn acquire(
        leases: &Arc<dyn LeaseStore>,
        shard: ShardId,
        requested_id: Option<String>,
        initial_lease: Option<WriterLease>,
        config: &AggregatorConfig,
    ) -> BpResult<Option<Self>> {
        let writer_id = requested_id
            .or_else(|| initial_lease.as_ref().map(|lease| lease.owner_id.clone()))
            .unwrap_or_else(WriterLease::new_writer_id);
        let lease = match initial_lease {
            Some(lease) => lease,
            None => tokio::select! {
                result = WriterLease::wait_for_lease(
                    leases.as_ref(),
                    shard,
                    &writer_id,
                    config.lease_ttl,
                ) => result?,
                signal = Runtime::shutdown_signal() => {
                    signal?;
                    return Ok(None);
                }
            },
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

    pub(super) fn id(&self) -> &str {
        &self.id
    }

    pub(super) fn lease(&self) -> &WriterLease {
        &self.lease
    }

    /// Extends the lease and replaces the local copy with the renewed one.
    pub(super) async fn renew(
        &mut self,
        leases: &Arc<dyn LeaseStore>,
        shard: ShardId,
        ttl: Duration,
    ) -> BpResult<()> {
        let Some(renewed) = leases.renew(&self.lease, ttl).await? else {
            return Err(BpError::bad_value(
                "aggregator lease",
                shard.to_string(),
                format!("writer {} lost its fencing lease", self.id),
            ));
        };
        self.lease = renewed;
        Ok(())
    }

    pub(super) async fn release(self, leases: &Arc<dyn LeaseStore>) -> BpResult<()> {
        leases.release(&self.lease).await
    }
}
