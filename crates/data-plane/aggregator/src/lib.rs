//! Market-data candle aggregation service.

mod candle_sink;
mod checkpoint_record;
mod config;
mod hash_router;
mod market_source;
mod processor;
mod restore;
mod runner;
mod shard_id;
mod state;
mod supervisor;
mod writer_lease;

#[cfg(test)]
mod candle_sink_tests;
#[cfg(test)]
mod checkpoint_record_tests;
#[cfg(test)]
mod hash_router_tests;
#[cfg(test)]
mod market_source_tests;
#[cfg(test)]
mod shard_id_tests;
#[cfg(test)]
mod supervisor_tests;
#[cfg(test)]
mod writer_lease_tests;

pub use candle_sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore};
pub use checkpoint_record::{CheckpointRecord, NatsStateStore, StateStore};
pub use config::{AggregatorConfig, ReplayStartPolicy};
pub use hash_router::{HashRouter, NatsRouter};
pub use market_source::{
    MarketInput, MarketSource, NatsMarketSource, NatsShardSource, SourceRecord,
};
pub use processor::{CandleProcessor, ProcessorConfig};
pub use restore::{
    ArchiveRecoveryConfig, ArchivedEventSource, RestoreOptions, RestoreSource, RestoredShard,
    S3ArchiveRecovery, S3ArchivedEventSource,
};
pub use runner::{ProcessorRunner, QuiesceReceiver, QuiesceSignal};
pub use shard_id::ShardId;
pub use state::{
    AggregatorState, DeduplicationKey, IncompleteWindow, KnownGap, MAX_SEEN_SEQUENCE_RANGES,
    STATE_SCHEMA_VERSION, SequenceRange, StreamProgress,
};
pub use writer_lease::{LeaseStore, NatsLeaseStore, WriterLease};
