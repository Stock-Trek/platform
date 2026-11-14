use crate::{
    LeaseStore, MarketSource, ProcessorRunner, StateStore,
    candle_sink::{ClosedCandlePublisher, OpenCandleStore},
    checkpoint_record_tests::InMemoryStateStore,
    config::AggregatorConfig,
    shard_id::ShardId,
    state::AggregatorState,
    writer_lease::InMemoryLeaseStore,
};
use std::sync::Arc;

impl<S, K> ProcessorRunner<S, K>
where
    S: MarketSource,
    K: OpenCandleStore + ClosedCandlePublisher + 'static,
{
    pub fn new(config: AggregatorConfig, source: S, sink: K) -> Self {
        let state = AggregatorState::new_with_schema_version(config.state_schema_version);
        Self {
            config,
            source,
            sink,
            checkpoints: Arc::new(InMemoryStateStore::new()),
            leases: Arc::new(InMemoryLeaseStore::new()),
            shard: ShardId::new(0),
            state,
            replay_watermark: None,
            writer_id: None,
            initial_lease: None,
            quiesce: None,
        }
    }

    pub fn with_checkpoint_store(
        mut self,
        checkpoints: Arc<dyn StateStore>,
        shard: ShardId,
    ) -> Self {
        self.checkpoints = checkpoints;
        self.shard = shard;
        self
    }

    pub fn with_lease_store(mut self, leases: Arc<dyn LeaseStore>) -> Self {
        self.leases = leases;
        self
    }
}
