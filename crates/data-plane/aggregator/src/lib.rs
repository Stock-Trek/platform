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

pub use checkpoint::{CheckpointRecord, NatsStateStore, StateStore};
pub use config::{AggregatorConfig, ReplayStartPolicy};
pub use lease::{LeaseStore, NatsLeaseStore, WriterLease};
pub use processor::{CandleProcessor, ProcessorConfig, ProcessorUpdate};
pub use restore::{
    AGGREGATOR_ARCHIVE_S3_BUCKET, AGGREGATOR_ARCHIVE_S3_PREFIX, ArchiveRecoveryConfig,
    ArchivedEventSource, EmptyArchivedEventSource, RestoreOptions, RestoreSource, RestoredShard,
    S3ArchiveRecovery, S3ArchivedEventSource,
};
pub use router::{HashRouter, NatsRouter};
pub use runner::{ProcessorRunner, QuiesceReceiver, QuiesceSignal};
pub use shard::ShardId;
pub use sink::{ClosedCandlePublisher, NatsCandleSink, OpenCandleStore};
pub use source::{MarketInput, MarketSource, NatsMarketSource, NatsShardSource, SourceRecord};
pub use state::{
    AggregatorState, DeduplicationKey, IncompleteWindow, KnownGap, MAX_SEEN_SEQUENCE_RANGES,
    STATE_SCHEMA_VERSION, SequenceRange, StreamProgress,
};
