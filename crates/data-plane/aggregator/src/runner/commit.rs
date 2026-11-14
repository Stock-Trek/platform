//! Checkpointing and source acknowledgement for a shard writer.

use crate::runner::{ProcessorRunner, output_writer::CandleOutputWriter, writer::Writer};
use crate::{
    candle_sink::{ClosedCandlePublisher, OpenCandleStore},
    checkpoint_record::{CheckpointRecord, StateStore},
    config::AggregatorConfig,
    market_source::{MarketSource, SourceRecord},
    processor::CandleProcessor,
    shard_id::ShardId,
    state::AggregatorState,
    writer_lease::LeaseStore,
};
use platform_errors::{BuildError, BuildResult, ErrorType};
use platform_metrics::AggregatorMetrics;
use platform_types::TimestampMilliseconds;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct CheckpointPosition {
    source_stream: String,
    next_sequence: u64,
}

pub struct CommitState {
    pub pending: Vec<SourceRecord>,
    pub last_record: Option<SourceRecord>,
    pub dirty: bool,
    pub last_checkpoint_at: Instant,
    pub resume_position: Option<CheckpointPosition>,
}

impl CommitState {
    pub fn new(resume_position: Option<CheckpointPosition>) -> Self {
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
    pub async fn resume_position(
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
    pub async fn commit(
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
    ) -> BuildResult<()> {
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
                    return Err(BuildError::bad_value(
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
                    return Err(BuildError::bad_value(
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
