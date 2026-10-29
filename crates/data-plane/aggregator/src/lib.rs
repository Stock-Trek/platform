//! Market-data candle aggregation service.

pub mod aggregator;
pub mod checkpoint;
pub mod config;
pub mod partition;
pub mod restore;
pub mod router;
pub mod runner;
pub mod sink;
pub mod source;
pub mod state;

pub use aggregator::{AggregationUpdate, CandleAggregator};
pub use checkpoint::{
    Checkpoint, CheckpointRecord, CheckpointStore, InMemoryCheckpointStore, InMemoryStateStore,
    NatsCheckpointStore, NatsStateStore, SourceCheckpoint, StateCheckpoint, StateStore,
};
pub use config::AggregatorConfig;
pub use partition::{PartitionId, all_partitions, partition_for, partition_for_candle_key};
pub use restore::{
    ArchiveRecoveryConfig, ArchivedEventSource, EmptyArchivedEventSource, RestoreResult,
    RestoredPartition, S3ArchivedEventSource, rebuild_state_from_archive, restore_partition,
};
pub use router::{HashRouter, NatsRouter, partitions_for_event};
pub use runner::AggregatorRunner;
pub use sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore};
pub use source::{MarketInput, MarketSource, NatsMarketSource, NatsPartitionSource, SourceRecord};
pub use state::{
    AggregatorState, IncompleteRange, KnownGap, SCHEMA_VERSION, STATE_SCHEMA_VERSION, SequenceRange,
};
