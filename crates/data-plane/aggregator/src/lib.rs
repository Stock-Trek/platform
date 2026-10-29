//! Market-data candle aggregation service.

pub mod aggregator;
pub mod checkpoint;
pub mod config;
pub mod partition;
pub mod router;
pub mod runner;
pub mod sink;
pub mod source;

pub use aggregator::{AggregationUpdate, CandleAggregator};
pub use checkpoint::{
    CheckpointStore, InMemoryCheckpointStore, NatsCheckpointStore, SourceCheckpoint,
};
pub use config::AggregatorConfig;
pub use partition::{PartitionId, all_partitions, partition_for, partition_for_candle_key};
pub use router::{HashRouter, NatsRouter, partitions_for_event};
pub use runner::AggregatorRunner;
pub use sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore};
pub use source::{MarketInput, MarketSource, NatsMarketSource, NatsPartitionSource, SourceRecord};
