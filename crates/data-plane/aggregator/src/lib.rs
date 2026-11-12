//! Market-data candle aggregation service.

pub mod checkpoint_record;

#[cfg(test)]
pub(crate) mod checkpoint_record_tests;

pub mod config;
pub mod writer_lease;

#[cfg(test)]
pub(crate) mod writer_lease_tests;

pub mod hash_router;
pub mod processor;
pub mod restore;

#[cfg(test)]
mod hash_router_tests;

pub mod runner;
pub mod shard_id;

#[cfg(test)]
mod shard_id_tests;

pub mod candle_sink;

#[cfg(test)]
mod candle_sink_tests;

pub mod market_source;

#[cfg(test)]
mod market_source_tests;

pub mod state;
pub mod supervisor;
#[cfg(test)]
mod supervisor_tests;

pub use candle_sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore};
pub use checkpoint_record::{CheckpointRecord, NatsStateStore, StateStore};
pub use config::{AggregatorConfig, ReplayStartPolicy};
pub use hash_router::{HashRouter, NatsRouter};
pub use market_source::{
    MarketInput, MarketSource, NatsMarketSource, NatsShardSource, SourceRecord,
};
pub use processor::{CandleProcessor, ProcessorConfig, ProcessorUpdate};
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
