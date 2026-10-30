//! Checkpointing and source acknowledgement for a shard writer.

use super::{ProcessorRunner, output::CandleOutputWriter, writer::Writer};
use crate::{
    checkpoint::{CheckpointRecord, StateStore},
    config::AggregatorConfig,
    lease::LeaseStore,
    processor::CandleProcessor,
    shard::ShardId,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::{MarketSource, SourceRecord},
    state::AggregatorState,
};
use platform_errors::{BpError, BpResult, ErrorType};
use platform_metrics::AggregatorMetrics;
use platform_types::new_types::TimestampMilliseconds;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

#[derive(Debug, Clone)]
pub(super) struct CheckpointPosition {
    source_stream: String,
    next_sequence: u64,
}

pub(super) struct CommitState {
    pub(super) pending: Vec<SourceRecord>,
    pub(super) last_record: Option<SourceRecord>,
    pub(super) dirty: bool,
    pub(super) last_checkpoint_at: Instant,
    pub(super) resume_position: Option<CheckpointPosition>,
}

impl CommitState {
    pub(super) fn new(resume_position: Option<CheckpointPosition>) -> Self {
        Self {
            pending: Vec::new(),
            last_record: None,
            dirty: false,
            last_checkpoint_at: Instant::now(),
            resume_position,
        }
    }
}

impl<S, K> ProcessorRunner<S, K>
where
    S: MarketSource,
    K: OpenCandleStore + ClosedCandlePublisher + 'static,
{
    pub(super) async fn resume_position(
        checkpoints: &Arc<dyn StateStore>,
        shard: ShardId,
        replay_watermark: Option<TimestampMilliseconds>,
        state: &AggregatorState,
    ) -> Option<CheckpointPosition> {
        if replay_watermark.is_some() {
            return None;
        }
        match checkpoints.load(shard).await {
            Ok(Some(checkpoint))
                if checkpoint.next_sequence > 0
                    && !checkpoint.source_stream.is_empty()
                    && checkpoint.state.last_processed_sequence
                        == state.last_processed_sequence
                    && checkpoint.state.watermark == state.watermark
                    && checkpoint.state.state_epoch == state.state_epoch =>
            {
                Some(CheckpointPosition {
                    source_stream: checkpoint.source_stream,
                    next_sequence: checkpoint.next_sequence,
                })
            }
            Ok(_) => None,
            Err(error) => {
                warn!(
                    %error,
                    shard = %shard,
                    "failed to load checkpoint position for idle checkpoints"
                );
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn commit(
        config: &AggregatorConfig,
        source: &mut S,
        checkpoints: &Arc<dyn StateStore>,
        leases: &Arc<dyn LeaseStore>,
        shard: ShardId,
        processor: &CandleProcessor,
        writer: &mut Writer,
        state: &mut CommitState,
        outputs: &CandleOutputWriter,
        drain_open: bool,
    ) -> BpResult<()> {
        writer.renew(leases, shard, config.lease_ttl).await?;

        // Durable outputs must reach the sink before the checkpoint that
        // advances past the inputs which produced them can be saved. Ordinary
        // checkpoints only need the closed-candle confirmation barrier; open
        // projections are derived and may still be coalescing. Final drains
        // before quiesce or lease release flush both.
        if drain_open {
            outputs.flush().await?;
        } else {
            outputs.barrier().await?;
        }

        AggregatorMetrics::record_checkpoint_age(shard.get(), state.last_checkpoint_at.elapsed());
        if !state.dirty && state.pending.is_empty() {
            return Ok(());
        }

        let checkpoint = match state.last_record.as_ref() {
            Some(record) => {
                let Some(checkpoint) = CheckpointRecord::from_message(
                    &record.message,
                    shard,
                    processor.state().clone(),
                )?
                else {
                    return Err(BpError::bad_value(
                        "aggregator checkpoint",
                        shard.to_string(),
                        "source record has no JetStream sequence to checkpoint",
                    ));
                };
                checkpoint
            }
            None => {
                let Some(position) = state.resume_position.as_ref() else {
                    if state.pending.is_empty() {
                        return Ok(());
                    }
                    return Err(BpError::bad_value(
                        "aggregator checkpoint",
                        shard.to_string(),
                        "cannot checkpoint without a source record",
                    ));
                };
                CheckpointRecord::new(
                    shard,
                    position.source_stream.clone(),
                    position.next_sequence,
                    processor.state().clone(),
                )
            }
        };
        if let Err(error) = checkpoints
            .save(&checkpoint.with_lease(writer.lease()))
            .await
        {
            AggregatorMetrics::record_checkpoint_failure(shard.get(), error.error_type());
            return Err(error);
        }
        state.last_checkpoint_at = Instant::now();
        AggregatorMetrics::record_checkpoint_age(shard.get(), state.last_checkpoint_at.elapsed());

        for record in state.pending.drain(..) {
            source.ack(&record).await?;
        }
        Ok(())
    }
}
