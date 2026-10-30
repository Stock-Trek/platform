//! Market-data candle aggregation service.

pub mod checkpoint;
pub mod config;
pub mod lease;
pub mod processor;
pub mod restore;
pub mod router;
pub mod runner;
pub mod shard;
pub mod sink;
pub mod source;
pub mod state;

pub use checkpoint::{CheckpointRecord, InMemoryStateStore, NatsStateStore, StateStore};
pub use config::{AggregatorConfig, ReplayStartPolicy};
pub use lease::{
    InMemoryLeaseStore, LeaseStore, NatsLeaseStore, WriterLease, new_writer_id, wait_for_lease,
};
pub use processor::{CandleProcessor, ProcessorConfig, ProcessorUpdate};
pub use restore::{
    ArchiveRecoveryConfig, ArchivedEventSource, EmptyArchivedEventSource, RestoreOptions,
    RestoreSource, RestoredShard, S3ArchiveRecovery, S3ArchivedEventSource,
    rebuild_state_from_archive, rebuild_state_from_archive_with_schema, restore_shard,
};
pub use router::{HashRouter, NatsRouter};
pub use runner::{ProcessorRunner, QuiesceReceiver, QuiesceSignal};
pub use shard::{ShardId, all_shards, shard_for, shard_for_candle_key};
pub use sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore};
pub use source::{MarketInput, MarketSource, NatsMarketSource, NatsShardSource, SourceRecord};
pub use state::{
    AggregatorState, DeduplicationKey, IncompleteWindow, KnownGap, STATE_SCHEMA_VERSION,
    SequenceRange, StreamProgress,
};
