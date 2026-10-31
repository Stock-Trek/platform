use super::*;
use crate::{
    config::AggregatorConfig,
    shard::ShardId,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::MarketSource,
    state::AggregatorState,
};
use std::sync::Arc;
use crate::{checkpoint::tests::InMemoryStateStore, lease::tests::InMemoryLeaseStore};

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
}
