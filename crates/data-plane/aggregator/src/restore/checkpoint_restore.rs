//! Checkpoint-first restore orchestration for aggregator shards.
//!
//! Recovery prefers the latest compatible checkpoint because it contains the
//! exact keyed state and the next source sequence. If the checkpoint is
//! missing, stale, or from an incompatible schema, the shard state is
//! rebuilt from the compacted event archive plus the raw tail that has not been
//! compacted yet.

use super::rebuild::RebuiltArchiveState;
use super::replay::ArchivedEventSource;
use super::restored_shard::{RestoreOptions, RestoreSource, RestoredShard};
use crate::checkpoint::StateStore;
use crate::config::ReplayStartPolicy;
use crate::processor::ProcessorConfig;
use crate::state::AggregatorState;
use platform_errors::{BuildResult, ErrorType};
use platform_metrics::AggregatorMetrics;
use platform_nats::ConsumerStart;
use platform_types::new_types::TimestampMilliseconds;
use std::future::Future;
use std::time::{Duration, Instant};
use tracing::{info, warn};

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
    ) -> BuildResult<RestoredShard>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = BuildResult<Box<dyn ArchivedEventSource>>>,
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
    ) -> BuildResult<RestoredShard>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = BuildResult<Box<dyn ArchivedEventSource>>>,
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
