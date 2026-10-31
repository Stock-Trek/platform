//! Shard processing worker lifecycle.

use super::{
    commit::CommitState, output::CandleOutputWriter, quiesce::QuiesceReceiver, writer::Writer,
};
use crate::{
    checkpoint::StateStore,
    config::AggregatorConfig,
    lease::{LeaseStore, WriterLease},
    processor::CandleProcessor,
    shard::ShardId,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::MarketSource,
    state::AggregatorState,
};
use platform_errors::BpResult;
use platform_metrics::AggregatorMetrics;
use platform_runtime::Runtime;
use platform_types::new_types::TimestampMilliseconds;
use std::sync::Arc;

/// Processes one shard's ordered inputs.
///
/// The worker acquires a fenced writer lease, applies inputs through the pure
/// [`CandleProcessor`], publishes outputs, checkpoints the keyed state
/// periodically and at shutdown, and only then acknowledges the source records.
///
/// A coordinator can install a [`super::QuiesceSignal`] with
/// [`ProcessorRunner::with_quiesce_signal`]. The running worker then stops
/// polling for new inputs, drains and checkpoints accepted records, and releases
/// its lease before exiting so ownership can move to another writer.
pub struct ProcessorRunner<S, K> {
    config: AggregatorConfig,
    source: S,
    sink: K,
    checkpoints: Arc<dyn StateStore>,
    leases: Arc<dyn LeaseStore>,
    shard: ShardId,
    state: AggregatorState,
    /// When set, inputs at or before this watermark are already represented by
    /// state rebuilt from the archive and are only used to advance the source
    /// position.
    replay_watermark: Option<TimestampMilliseconds>,
    writer_id: Option<String>,
    initial_lease: Option<WriterLease>,
    quiesce: Option<QuiesceReceiver>,
}

impl<S, K> ProcessorRunner<S, K>
where
    S: MarketSource,
    K: OpenCandleStore + ClosedCandlePublisher + 'static,
{
    pub fn with_checkpoint_store(
        mut self,
        checkpoints: Arc<dyn StateStore>,
        shard: ShardId,
    ) -> Self {
        self.checkpoints = checkpoints;
        self.shard = shard;
        self
    }

    pub fn with_lease_store(mut self, leases: Arc<dyn LeaseStore>) -> Self {
        self.leases = leases;
        self
    }

    pub fn with_writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Uses a lease acquired by the caller instead of waiting for one here.
    pub fn with_lease(mut self, lease: WriterLease) -> Self {
        self.initial_lease = Some(lease);
        self
    }

    /// Installs the signal a rebalance coordinator uses to drain this worker.
    pub fn with_quiesce_signal(mut self, signal: impl Into<QuiesceReceiver>) -> Self {
        self.quiesce = Some(signal.into());
        self
    }

    /// Installs an already-created quiesce receiver.
    pub fn with_quiesce_receiver(self, receiver: QuiesceReceiver) -> Self {
        self.with_quiesce_signal(receiver)
    }

    /// Restores keyed state, for example from a checkpoint or archive rebuild.
    pub fn with_state(mut self, state: AggregatorState) -> Self {
        self.state = state;
        self
    }

    /// Ignores live inputs at or before `watermark` because their effects are
    /// already present in restored state.
    pub fn with_replay_watermark(mut self, watermark: Option<TimestampMilliseconds>) -> Self {
        self.replay_watermark = watermark;
        self
    }

    pub fn new_shard(
        config: AggregatorConfig,
        source: S,
        sink: K,
        checkpoints: Arc<dyn StateStore>,
        leases: Arc<dyn LeaseStore>,
        shard: ShardId,
    ) -> Self {
        let state = AggregatorState::new_with_schema_version(config.state_schema_version);
        Self {
            config,
            source,
            sink,
            checkpoints,
            leases,
            shard,
            state,
            replay_watermark: None,
            writer_id: None,
            initial_lease: None,
            quiesce: None,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            mut source,
            sink,
            checkpoints,
            leases,
            shard,
            state,
            replay_watermark,
            writer_id,
            initial_lease,
            quiesce,
        } = self;
        let mut processor = CandleProcessor::from_state(config.processor_config(), state)?;
        for interval in &processor.config().intervals {
            AggregatorMetrics::record_retained_closed_candles(
                shard.get(),
                &interval.label(),
                processor
                    .state()
                    .closed_candles_len_by_interval()
                    .get(interval)
                    .copied()
                    .unwrap_or_default(),
            );
        }
        let Some(mut writer) =
            Writer::acquire(&leases, shard, writer_id, initial_lease, &config).await?
        else {
            return Ok(());
        };

        let resume_position =
            Self::resume_position(&checkpoints, shard, replay_watermark, processor.state()).await;
        let mut commit_state = CommitState::new(resume_position);
        let outputs = CandleOutputWriter::start(sink);
        let result = Runtime::run_until_shutdown(Self::process_inputs(
            &config,
            &mut source,
            &checkpoints,
            &leases,
            shard,
            &mut processor,
            replay_watermark,
            &mut writer,
            quiesce,
            &mut commit_state,
            &outputs,
        ))
        .await;
        let commit_result = if result.is_ok() {
            Self::commit(
                &config,
                &mut source,
                &checkpoints,
                &leases,
                shard,
                &processor,
                &mut writer,
                &mut commit_state,
                &outputs,
                true,
            )
            .await
        } else {
            Ok(())
        };
        let shutdown_result = outputs.shutdown().await;
        let release_result = writer.release(&leases).await;

        result?;
        commit_result?;
        shutdown_result?;
        release_result?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
