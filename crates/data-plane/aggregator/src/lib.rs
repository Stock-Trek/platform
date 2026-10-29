//! Market-data candle aggregation service.

pub mod checkpoint;
pub mod config;
pub mod lease;
pub mod partition;
pub mod processor;
pub mod restore;
pub mod router;
pub mod runner;
pub mod sink;
pub mod source;
pub mod state;

pub use checkpoint::{
    Checkpoint, CheckpointRecord, CheckpointStore, InMemoryCheckpointStore, InMemoryStateStore,
    NatsCheckpointStore, NatsStateStore, SourceCheckpoint, StateCheckpoint, StateStore,
};
pub use config::{AggregatorConfig, ReplayStartPolicy};
pub use lease::{InMemoryLeaseStore, LeaseStore, NatsLeaseStore, WriterLease, new_writer_id};
pub use partition::{PartitionId, all_partitions, partition_for, partition_for_candle_key};
pub use processor::{CandleProcessor, ProcessorConfig, ProcessorUpdate};
pub use restore::{
    ArchiveRecoveryConfig, ArchivedEventSource, EmptyArchivedEventSource, RestoreOptions,
    RestoreResult, RestoreSource, RestoredPartition, S3ArchivedEventSource,
    rebuild_state_from_archive, restore_partition,
};
pub use router::{HashRouter, NatsRouter, partitions_for_event};
pub use runner::ProcessorRunner;
pub use sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore};
pub use source::{MarketInput, MarketSource, NatsMarketSource, NatsPartitionSource, SourceRecord};
pub use state::{
    AggregatorState, DeduplicationKey, IncompleteRange, IncompleteWindow, KnownGap, SCHEMA_VERSION,
    STATE_SCHEMA_VERSION, SequenceRange, StreamProgress,
};
