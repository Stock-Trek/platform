//! Checkpoint restore and archive-backed state rebuild for aggregator
//! partitions.
//!
//! Recovery prefers the latest compatible checkpoint because it contains the
//! exact keyed state and the next source sequence. If the checkpoint is
//! missing, stale, or from an incompatible schema, the partition state is
//! rebuilt from the compacted event archive plus the raw tail that has not been
//! compacted yet.

use super::replay::{ArchivedEventSource, decoder_for};
use crate::checkpoint::StateStore;
use crate::config::{AggregatorConfig, ReplayStartPolicy};
use crate::partition::{PartitionId, partition_for};
use crate::processor::{CandleProcessor, ProcessorConfig};
use crate::state::{AggregatorState, STATE_SCHEMA_VERSION};
use platform_errors::{BpResult, ErrorType};
use platform_market_data::candle::CandleInterval;
use platform_metrics::AggregatorMetrics;
use platform_nats::ConsumerStart;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// The state and source position a partition should start from.
#[derive(Debug, Clone)]
pub struct RestoredPartition {
    pub state: AggregatorState,
    /// Where the live source should begin after recovery.
    pub start: ConsumerStart,
    /// Which recovery path produced the state.
    pub restored_from: RestoreSource,
    /// Whether the state was rebuilt by replaying archived raw events.
    pub rebuilt_from_archive: bool,
}

/// Recovery path that produced a partition's starting state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreSource {
    Checkpoint,
    Archive,
    Empty,
}

impl RestoreSource {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::Archive => "archive",
            Self::Empty => "empty",
        }
    }
}

impl RestoredPartition {
    pub fn replay_watermark(&self) -> Option<TimestampMilliseconds> {
        if self.rebuilt_from_archive {
            self.state.watermark
        } else {
            None
        }
    }

    pub fn start_sequence(&self) -> Option<u64> {
        match self.start {
            ConsumerStart::Sequence(sequence) => Some(sequence),
            ConsumerStart::Earliest | ConsumerStart::Latest => None,
        }
    }
}

/// Inputs that control checkpoint restore and archive rebuild for one writer.
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    pub partition: PartitionId,
    pub source_stream: String,
    pub intervals: Vec<CandleInterval>,
    pub partition_count: usize,
    pub allowed_lateness: DurationMilliseconds,
    pub replay_start_policy: ReplayStartPolicy,
    pub expected_schema_version: u32,
}

impl RestoreOptions {
    pub fn from_config(
        config: &AggregatorConfig,
        partition: PartitionId,
        source_stream: impl Into<String>,
    ) -> Self {
        Self {
            partition,
            source_stream: source_stream.into(),
            intervals: config.intervals.clone(),
            partition_count: config.shard_count,
            allowed_lateness: config.processor_config().allowed_lateness,
            replay_start_policy: config.replay_start_policy,
            expected_schema_version: config.state_schema_version,
        }
    }
}

/// Restores a partition from its latest checkpoint, rebuilds it from archived
/// raw events, or starts with empty state according to the configured policy.
///
/// A checkpoint is accepted only when it belongs to this partition, names the
/// same source stream, and carries the expected state schema. Otherwise the
/// archive is replayed from the beginning and the caller starts the live source
/// from the beginning as well. Events at or before the rebuilt watermark are
/// then ignored by [`crate::runner::ProcessorRunner`] to avoid applying state
/// twice.
///
/// `connect_archive` is only invoked when archive replay is required, so a
/// valid checkpoint or an `earliest`/`latest` start performs no archive I/O.
pub async fn restore_partition<F, Fut>(
    store: &dyn StateStore,
    connect_archive: F,
    options: &RestoreOptions,
) -> BpResult<RestoredPartition>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = BpResult<Box<dyn ArchivedEventSource>>>,
{
    let started = Instant::now();
    let result = restore_partition_inner(store, connect_archive, options).await;
    let outcome = match &result {
        Ok(restored) => restored.restored_from.label(),
        Err(_) => "error",
    };
    AggregatorMetrics::record_state_restore(options.partition.get(), outcome, started.elapsed());
    result
}

async fn restore_partition_inner<F, Fut>(
    store: &dyn StateStore,
    connect_archive: F,
    options: &RestoreOptions,
) -> BpResult<RestoredPartition>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = BpResult<Box<dyn ArchivedEventSource>>>,
{
    let partition = options.partition;
    match options.replay_start_policy {
        ReplayStartPolicy::Earliest | ReplayStartPolicy::Latest => {
            let start = match options.replay_start_policy {
                ReplayStartPolicy::Latest => ConsumerStart::Latest,
                _ => ConsumerStart::Earliest,
            };
            info!(
                partition = %partition,
                policy = options.replay_start_policy.label(),
                "starting aggregator partition with empty state"
            );
            return Ok(RestoredPartition {
                state: AggregatorState::new_with_schema_version(options.expected_schema_version),
                start,
                restored_from: RestoreSource::Empty,
                rebuilt_from_archive: false,
            });
        }
        ReplayStartPolicy::Archive => {
            info!(
                partition = %partition,
                "archive replay policy selected; ignoring checkpoint"
            );
        }
        ReplayStartPolicy::Checkpoint => match store.load(partition).await {
            Ok(Some(checkpoint))
                if checkpoint.is_compatible_with_schema(
                    partition,
                    &options.source_stream,
                    options.expected_schema_version,
                ) =>
            {
                let age = checkpoint.age(TimestampMilliseconds::now());
                AggregatorMetrics::record_checkpoint_age(
                    partition.get(),
                    Duration::from_millis(age.0.max(0) as u64),
                );
                info!(
                    partition = %partition,
                    next_sequence = checkpoint.next_sequence,
                    watermark = ?checkpoint.watermark,
                    open_candles = checkpoint.state.open_candles_len(),
                    "restored aggregator partition from checkpoint"
                );
                return Ok(RestoredPartition {
                    state: checkpoint.state,
                    start: ConsumerStart::Sequence(checkpoint.next_sequence),
                    restored_from: RestoreSource::Checkpoint,
                    rebuilt_from_archive: false,
                });
            }
            Ok(Some(checkpoint)) => {
                warn!(
                    partition = %partition,
                    source_stream = %checkpoint.source_stream,
                    schema_version = checkpoint.state.schema_version,
                    expected_schema_version = options.expected_schema_version,
                    "ignoring stale or incompatible checkpoint; rebuilding from archive"
                );
            }
            Ok(None) => {
                info!(
                    partition = %partition,
                    "no checkpoint found; rebuilding from archive"
                );
            }
            Err(error) => {
                AggregatorMetrics::record_checkpoint_failure(partition.get(), error.error_type());
                warn!(
                    partition = %partition,
                    error = %error,
                    "failed to load checkpoint; rebuilding from archive"
                );
            }
        },
    }

    AggregatorMetrics::record_replay_progress(partition.get(), "archive", 0.0);
    let mut archive = connect_archive().await?;
    let state = rebuild_state_from_archive_with_schema(
        archive.as_mut(),
        partition,
        &options.intervals,
        options.partition_count,
        options.allowed_lateness,
        options.expected_schema_version,
    )
    .await?;
    AggregatorMetrics::record_replay_progress(partition.get(), "archive", 1.0);
    info!(
        partition = %partition,
        watermark = ?state.watermark,
        open_candles = state.open_candles_len(),
        "rebuilt aggregator partition state from archived raw events"
    );
    Ok(RestoredPartition {
        state,
        start: ConsumerStart::Earliest,
        restored_from: RestoreSource::Archive,
        rebuilt_from_archive: true,
    })
}

/// Replays all archived events for one partition into a fresh keyed state.
pub async fn rebuild_state_from_archive(
    archive: &mut dyn ArchivedEventSource,
    partition: PartitionId,
    intervals: &[CandleInterval],
    partition_count: usize,
    allowed_lateness: DurationMilliseconds,
) -> BpResult<AggregatorState> {
    rebuild_state_from_archive_with_schema(
        archive,
        partition,
        intervals,
        partition_count,
        allowed_lateness,
        STATE_SCHEMA_VERSION,
    )
    .await
}

/// Replays all archived events for one partition into fresh keyed state with
/// the configured schema version.
pub async fn rebuild_state_from_archive_with_schema(
    archive: &mut dyn ArchivedEventSource,
    partition: PartitionId,
    intervals: &[CandleInterval],
    partition_count: usize,
    allowed_lateness: DurationMilliseconds,
    state_schema_version: u32,
) -> BpResult<AggregatorState> {
    let config = ProcessorConfig::new(intervals.to_vec(), allowed_lateness)
        .with_state_schema_version(state_schema_version);
    let mut processor = CandleProcessor::with_config(config);
    let mut decoders = HashMap::new();
    let mut replayed = 0_u64;

    while let Some(archived) = archive.next_event().await? {
        replayed = replayed.saturating_add(1);
        if replayed.is_multiple_of(1_000) {
            AggregatorMetrics::record_replay_events(partition.get(), "archive", 1_000);
        }
        let event = &archived.event;
        let decoder = decoder_for(&mut decoders, event.exchange)?;
        let trades = match decoder.decode(event) {
            Ok(trades) => trades
                .into_iter()
                .filter(|trade| {
                    partition_for(trade.exchange, trade.trading_pair, partition_count) == partition
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                warn!(%error, stream = %event.stream, "skipping unreadable archived event");
                Vec::new()
            }
        };
        // Closed candles produced during rebuild are already represented by
        // the archive's candle records; the fold reconstructs the same keyed
        // state, deduplication ranges, gaps, and event-time watermarks. Events
        // with no trades for this partition must not move its watermark.
        if !trades.is_empty() {
            let state = processor.state_mut();
            state.state_epoch = state.state_epoch.max(archived.state_epoch);
            let _ = processor.apply_event(event, &trades);
        }
    }
    AggregatorMetrics::record_replay_events(partition.get(), "archive", replayed % 1_000);

    Ok(processor.into_state())
}
