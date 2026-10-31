use super::*;
use crate::{
    StateStore,
    checkpoint::tests::InMemoryStateStore,
    config::AggregatorConfig,
    lease::tests::InMemoryLeaseStore,
    shard::ShardId,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::MarketSource,
    state::AggregatorState,
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
}
