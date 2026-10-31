//! Checkpoint restore and archive-backed state rebuild for aggregator
//! shards.
//!
//! Recovery prefers the latest compatible checkpoint because it contains the
//! exact keyed state and the next source sequence. If the checkpoint is
//! missing, stale, or from an incompatible schema, the shard state is
//! rebuilt from the compacted event archive plus the raw tail that has not been
//! compacted yet.

use super::replay::{ArchivedEventSource, S3ArchivedEventSource};
use crate::checkpoint::StateStore;
use crate::config::{AggregatorConfig, ReplayStartPolicy};
use crate::processor::{CandleProcessor, ProcessorConfig};
use crate::shard::ShardId;
use crate::state::AggregatorState;
use platform_errors::{BpResult, ErrorType};
use platform_market_data::candle::CandleInterval;
use platform_metrics::AggregatorMetrics;
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::ConsumerStart;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// The state and source position a shard should start from.
#[derive(Debug, Clone)]
pub struct RestoredShard {
    pub state: AggregatorState,
    /// Where the live source should begin after recovery.
    pub start: ConsumerStart,
    /// Which recovery path produced the state.
    pub restored_from: RestoreSource,
    /// Whether the state was rebuilt by replaying archived raw events.
    pub rebuilt_from_archive: bool,
}

/// Recovery path that produced a shard's starting state.
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

impl RestoredShard {
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
    pub shard: ShardId,
    pub source_stream: String,
    pub intervals: Vec<CandleInterval>,
    pub shard_count: usize,
    pub allowed_lateness: DurationMilliseconds,
    pub max_closed_candles_per_interval: BTreeMap<CandleInterval, usize>,
    pub correction_cutoff: DurationMilliseconds,
    pub replay_start_policy: ReplayStartPolicy,
    pub expected_schema_version: u32,
}

impl RestoreOptions {
    pub fn from_config(
        config: &AggregatorConfig,
        shard: ShardId,
        source_stream: impl Into<String>,
    ) -> Self {
        Self {
            shard,
            source_stream: source_stream.into(),
            intervals: config.intervals.clone(),
            shard_count: config.shard_count,
            allowed_lateness: config.processor_config().allowed_lateness,
            max_closed_candles_per_interval: config
                .processor_config()
                .max_closed_candles_per_interval,
            correction_cutoff: config.processor_config().correction_cutoff,
            replay_start_policy: config.replay_start_policy,
            expected_schema_version: config.state_schema_version,
        }
    }
}

impl RestoreOptions {
    /// Restores a shard from its latest checkpoint, rebuilds it from archived
    /// raw events, or starts with empty state according to the configured policy.
    ///
    /// A checkpoint is accepted only when it belongs to this shard, names the
    /// same source stream, and carries the expected state schema. Otherwise the
    /// archive is replayed from the beginning and the caller starts the live source
    /// from the beginning as well. Events at or before the rebuilt watermark are
    /// then ignored by [`crate::runner::ProcessorRunner`] to avoid applying state
    /// twice.
    ///
    /// `connect_archive` is only invoked when archive replay is required, so a
    /// valid checkpoint or an `earliest`/`latest` start performs no archive I/O.
    pub async fn restore<F, Fut>(
        &self,
        store: &dyn StateStore,
        connect_archive: F,
    ) -> BpResult<RestoredShard>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = BpResult<Box<dyn ArchivedEventSource>>>,
    {
        let started = Instant::now();
        let result = self.restore_inner(store, connect_archive).await;
        let outcome = match &result {
            Ok(restored) => restored.restored_from.label(),
            Err(_) => "error",
        };
        AggregatorMetrics::record_state_restore(self.shard.get(), outcome, started.elapsed());
        result
    }

    async fn restore_inner<F, Fut>(
        &self,
        store: &dyn StateStore,
        connect_archive: F,
    ) -> BpResult<RestoredShard>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = BpResult<Box<dyn ArchivedEventSource>>>,
    {
        let shard = self.shard;
        match self.replay_start_policy {
            ReplayStartPolicy::Earliest | ReplayStartPolicy::Latest => {
                let start = match self.replay_start_policy {
                    ReplayStartPolicy::Latest => ConsumerStart::Latest,
                    _ => ConsumerStart::Earliest,
                };
                info!(
                    shard = %shard,
                    policy = self.replay_start_policy.label(),
                    "starting aggregator shard with empty state"
                );
                return Ok(RestoredShard {
                    state: AggregatorState::new_with_schema_version(self.expected_schema_version),
                    start,
                    restored_from: RestoreSource::Empty,
                    rebuilt_from_archive: false,
                });
            }
            ReplayStartPolicy::Archive => {
                info!(
                    shard = %shard,
                    "archive replay policy selected; ignoring checkpoint"
                );
            }
            ReplayStartPolicy::Checkpoint => match store.load(shard).await {
                Ok(Some(checkpoint))
                    if checkpoint.is_compatible_with_schema(
                        shard,
                        &self.source_stream,
                        self.expected_schema_version,
                    ) && checkpoint.state.candles_use_intervals(&self.intervals) =>
                {
                    let age = checkpoint.age(TimestampMilliseconds::now());
                    let next_sequence = checkpoint.next_sequence;
                    let mut state = checkpoint.state;
                    let evicted = state
                        .prune_closed_candles_to_caps(&self.max_closed_candles_per_interval)?;
                    if !evicted.is_empty() {
                        warn!(
                            shard = %shard,
                            evicted = evicted.len(),
                            "pruned restored checkpoint candles to the configured per-interval caps"
                        );
                    }
                    if let Some(watermark) = state.watermark {
                        state.prune_closed_windows(watermark - self.allowed_lateness);
                    }
                    AggregatorMetrics::record_checkpoint_age(
                        shard.get(),
                        Duration::from_millis(age.0.max(0) as u64),
                    );
                    info!(
                        shard = %shard,
                        next_sequence,
                        watermark = ?checkpoint.watermark,
                        open_candles = state.open_candles_len(),
                        "restored aggregator shard from checkpoint"
                    );
                    return Ok(RestoredShard {
                        state,
                        start: ConsumerStart::Sequence(next_sequence),
                        restored_from: RestoreSource::Checkpoint,
                        rebuilt_from_archive: false,
                    });
                }
                Ok(Some(checkpoint)) => {
                    warn!(
                        shard = %shard,
                        source_stream = %checkpoint.source_stream,
                        schema_version = checkpoint.state.schema_version,
                        expected_schema_version = self.expected_schema_version,
                        configured_intervals = ?self.intervals,
                        "ignoring stale, incompatible, or unconfigured-interval checkpoint; rebuilding from archive"
                    );
                }
                Ok(None) => {
                    info!(
                        shard = %shard,
                        "no checkpoint found; rebuilding from archive"
                    );
                }
                Err(error) => {
                    AggregatorMetrics::record_checkpoint_failure(shard.get(), error.error_type());
                    if error.retryability().is_retryable() {
                        warn!(
                            shard = %shard,
                            error = %error,
                            "transient checkpoint load failure; retrying instead of rebuilding from archive"
                        );
                        return Err(error);
                    }
                    warn!(
                        shard = %shard,
                        error = %error,
                        "permanent checkpoint load failure; rebuilding from archive"
                    );
                }
            },
        }

        AggregatorMetrics::record_replay_progress(shard.get(), "archive", 0.0);
        let mut archive = connect_archive().await?;
        let config = ProcessorConfig::new(
            self.intervals.clone(),
            self.allowed_lateness,
            self.max_closed_candles_per_interval.clone(),
            self.correction_cutoff,
        )
        .with_state_schema_version(self.expected_schema_version);
        let rebuilt =
            RebuiltArchiveState::rebuild(archive.as_mut(), shard, self.shard_count, config).await?;
        AggregatorMetrics::record_replay_progress(shard.get(), "archive", 1.0);

        if rebuilt.replayed_events == 0 {
            warn!(
                shard = %shard,
                policy = self.replay_start_policy.label(),
                "archive contains no events; starting with explicit empty state"
            );
            return Ok(RestoredShard {
                state: rebuilt.state,
                start: ConsumerStart::Earliest,
                restored_from: RestoreSource::Empty,
                rebuilt_from_archive: false,
            });
        }

        let state = rebuilt.state;
        info!(
            shard = %shard,
            watermark = ?state.watermark,
            open_candles = state.open_candles_len(),
            "rebuilt aggregator shard state from archived raw events"
        );
        Ok(RestoredShard {
            state,
            start: ConsumerStart::Earliest,
            restored_from: RestoreSource::Archive,
            rebuilt_from_archive: true,
        })
    }
}

struct RebuiltArchiveState {
    state: AggregatorState,
    replayed_events: u64,
}

impl AggregatorState {
    /// Replays all archived events for one shard into a fresh keyed state.
    pub async fn rebuild_from_archive(
        archive: &mut dyn ArchivedEventSource,
        shard: ShardId,
        intervals: &[CandleInterval],
        shard_count: usize,
        allowed_lateness: DurationMilliseconds,
        max_closed_candles_per_interval: BTreeMap<CandleInterval, usize>,
        correction_cutoff: DurationMilliseconds,
    ) -> BpResult<Self> {
        let config = ProcessorConfig::new(
            intervals.to_vec(),
            allowed_lateness,
            max_closed_candles_per_interval,
            correction_cutoff,
        );
        Ok(
            RebuiltArchiveState::rebuild(archive, shard, shard_count, config)
                .await?
                .state,
        )
    }

    /// Replays all archived events for one shard into fresh keyed state with
    /// the configured schema version.
    ///
    /// Archive recovery continues the highest state epoch observed on the
    /// replayed events instead of starting a new output lineage. Replaying the
    /// same archived inputs therefore reproduces the same deterministic output
    /// ids; callers that intentionally regenerate outputs under a new lineage,
    /// such as the rebuilder, must assign a fresh epoch themselves.
    #[allow(clippy::too_many_arguments)]
    pub async fn rebuild_from_archive_with_schema(
        archive: &mut dyn ArchivedEventSource,
        shard: ShardId,
        intervals: &[CandleInterval],
        shard_count: usize,
        allowed_lateness: DurationMilliseconds,
        max_closed_candles_per_interval: BTreeMap<CandleInterval, usize>,
        correction_cutoff: DurationMilliseconds,
        state_schema_version: u32,
    ) -> BpResult<Self> {
        let config = ProcessorConfig::new(
            intervals.to_vec(),
            allowed_lateness,
            max_closed_candles_per_interval,
            correction_cutoff,
        )
        .with_state_schema_version(state_schema_version);
        Ok(
            RebuiltArchiveState::rebuild(archive, shard, shard_count, config)
                .await?
                .state,
        )
    }
}

impl RebuiltArchiveState {
    async fn rebuild(
        archive: &mut dyn ArchivedEventSource,
        shard: ShardId,
        shard_count: usize,
        config: ProcessorConfig,
    ) -> BpResult<Self> {
        let mut processor = CandleProcessor::with_config(config);
        let mut decoders = HashMap::new();
        let mut replayed = 0_u64;

        while let Some(archived) = archive.next_event().await? {
            replayed = replayed.saturating_add(1);
            if replayed.is_multiple_of(1_000) {
                AggregatorMetrics::record_replay_events(shard.get(), "archive", 1_000);
            }
            let event = &archived.event;
            let trades = match S3ArchivedEventSource::decoder_for(&mut decoders, event.exchange) {
                Ok(decoder) => match decoder.decode(event) {
                    Ok(trades) => Some(ShardId::trades_for_shard(trades, shard, shard_count)),
                    Err(error) => {
                        MarketDataMetrics::record_decode_failure(
                            STAGE_AGGREGATOR,
                            DECODE_KIND_TRADE,
                        );
                        warn!(%error, stream = %event.stream, "skipping unreadable archived event");
                        None
                    }
                },
                Err(error) => {
                    MarketDataMetrics::record_dropped_event(
                        STAGE_AGGREGATOR,
                        DROP_REASON_NO_DECODER,
                    );
                    warn!(%error, exchange = %event.exchange, "no trade decoder for archived exchange");
                    None
                }
            };
            // Closed candles produced during rebuild are already represented by
            // the archive's candle records; the fold reconstructs the same keyed
            // state, deduplication ranges, gaps, and event-time watermarks. Events
            // that decode but have no trades for this shard must not move its
            // watermark. Undecodable events are still folded with no trades so
            // archive recovery matches live dispatch.
            if trades.as_ref().is_none_or(|trades| !trades.is_empty()) {
                // Recovery continues the lineage that produced the archived inputs,
                // so the same processing step produces the same deterministic
                // revision ids. A new epoch is reserved for an explicit output
                // rebuild rather than ordinary state recovery.
                let state = processor.state_mut();
                state.state_epoch = state.state_epoch.max(archived.state_epoch);
                let _ = processor.apply_event(event, trades.as_deref().unwrap_or_default());
            }
        }
        AggregatorMetrics::record_replay_events(shard.get(), "archive", replayed % 1_000);

        Ok(RebuiltArchiveState {
            state: processor.into_state(),
            replayed_events: replayed,
        })
    }
}
