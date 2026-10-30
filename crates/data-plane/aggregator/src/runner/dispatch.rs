//! Source input dispatch, candle publishing and idle flushing.

use super::{
    ProcessorRunner, QuiesceReceiver, commit::CommitState, output::CandleOutputWriter,
    writer::Writer,
};
use crate::{
    checkpoint::StateStore,
    config::AggregatorConfig,
    lease::LeaseStore,
    partition::PartitionId,
    processor::{CandleProcessor, ProcessorUpdate},
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::{MarketInput, MarketSource},
    state::DeduplicationKey,
};
use platform_errors::BpResult;
use platform_market_data::{
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::{GapMarker, MarketEvent},
};
use platform_metrics::AggregatorMetrics;
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

impl<S, K> ProcessorRunner<S, K>
where
    S: MarketSource,
    K: OpenCandleStore + ClosedCandlePublisher + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn process_inputs(
        config: &AggregatorConfig,
        source: &mut S,
        checkpoints: &Arc<dyn StateStore>,
        leases: &Arc<dyn LeaseStore>,
        partition: PartitionId,
        processor: &mut CandleProcessor,
        replay_watermark: Option<TimestampMilliseconds>,
        writer: &mut Writer,
        mut quiesce: Option<QuiesceReceiver>,
        state: &mut CommitState,
        outputs: &CandleOutputWriter,
    ) -> BpResult<()> {
        let mut decoders: HashMap<CexId, SharedTradeDecoder> = HashMap::new();
        let mut checkpoint_ticker = time::interval(config.checkpoint_interval);
        checkpoint_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        checkpoint_ticker.tick().await;
        let mut idle_ticker = time::interval(config.watermark_idle_timeout);
        idle_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        idle_ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = Self::wait_for_quiesce(quiesce.as_mut()) => {
                    AggregatorMetrics::record_rebalance(
                        partition.get(),
                        "quiesce_requested",
                    );
                    info!(
                        partition = %partition,
                        writer = %writer.id(),
                        "quiesce requested; draining and checkpointing aggregator partition"
                    );
                    break;
                }
                maybe_record = source.next_input() => {
                    match maybe_record {
                        Ok(Some(record)) => {
                            let is_replay =
                                Self::is_replay(processor, replay_watermark, &record.input);
                            // Mark the offset before folding the input so any
                            // candles closed by this record derive their
                            // revision from the source offset that produced
                            // them.
                            if let Some(sequence) = record.message.stream_sequence {
                                processor.state_mut().mark_processed(sequence);
                                state.dirty = true;
                            }
                            if !is_replay {
                                let update = Self::process_input(
                                    processor,
                                    &mut decoders,
                                    &record.input,
                                );
                                Self::record_update_metrics(&update);
                                outputs
                                    .enqueue(update, processor.state())
                                    .await?;
                                state.dirty = true;
                            } else {
                                AggregatorMetrics::record_replay_events(
                                    partition.get(),
                                    "live",
                                    1,
                                );
                            }
                            Self::record_watermark_lag(partition, processor);
                            idle_ticker.reset();
                            state.last_record = Some(record.clone());
                            state.pending.push(record);
                        }
                        Ok(None) => {
                            info!("aggregator input source ended; draining partition");
                            break;
                        }
                        Err(error) => {
                            error!(%error, "aggregator input source stopped");
                            return Err(error);
                        }
                    }
                }
                _ = checkpoint_ticker.tick() => {
                    Self::commit(
                        config,
                        source,
                        checkpoints,
                        leases,
                        partition,
                        processor,
                        writer,
                        state,
                        outputs,
                        false,
                    )
                    .await?;
                    state.dirty = false;
                }
                _ = idle_ticker.tick() => {
                    let previous_watermark = processor.state().watermark;
                    let update = Self::flush_idle(processor);
                    let changed = !update.is_empty()
                        || processor.state().watermark != previous_watermark;
                    outputs
                        .enqueue(update, processor.state())
                        .await?;
                    if changed {
                        state.dirty = true;
                    }
                    Self::record_watermark_lag(partition, processor);
                    Self::commit(
                        config,
                        source,
                        checkpoints,
                        leases,
                        partition,
                        processor,
                        writer,
                        state,
                        outputs,
                        false,
                    )
                    .await?;
                    state.dirty = false;
                }
            }
        }
        Ok(())
    }

    async fn wait_for_quiesce(quiesce: Option<&mut QuiesceReceiver>) {
        match quiesce {
            Some(receiver) => receiver.wait().await,
            None => std::future::pending().await,
        }
    }

    fn is_replay(
        processor: &CandleProcessor,
        replay_watermark: Option<TimestampMilliseconds>,
        input: &MarketInput,
    ) -> bool {
        let Some(watermark) = replay_watermark else {
            return false;
        };
        if input_timestamp(input) > watermark {
            return false;
        }
        match input {
            MarketInput::Event(event) => event.sequence.is_none_or(|sequence| {
                processor.state().is_duplicate(&DeduplicationKey::new(
                    event.exchange,
                    &event.stream,
                    sequence,
                ))
            }),
            MarketInput::Gap(_) => true,
        }
    }

    fn process_input(
        processor: &mut CandleProcessor,
        decoders: &mut HashMap<CexId, SharedTradeDecoder>,
        input: &MarketInput,
    ) -> ProcessorUpdate {
        match input {
            MarketInput::Event(event) => Self::process_event(processor, decoders, event),
            MarketInput::Gap(gap) => Self::process_gap(processor, gap),
        }
    }

    fn record_update_metrics(update: &ProcessorUpdate) {
        AggregatorMetrics::record_duplicate_events(update.duplicates as u64);
        AggregatorMetrics::record_incomplete_windows(update.incomplete_windows.len() as u64);
    }

    fn record_watermark_lag(partition: PartitionId, processor: &CandleProcessor) {
        let Some(watermark) = processor.state().watermark else {
            return;
        };
        let lag = TimestampMilliseconds::now() - watermark;
        AggregatorMetrics::record_watermark_lag(partition.get(), lag.0.max(0) as f64 / 1_000.0);
    }

    fn flush_idle(processor: &mut CandleProcessor) -> ProcessorUpdate {
        // Idle ticks must not advance event time to the wall clock. Doing so
        // closes old windows early and can make later backfills create
        // partial correction candles instead of revising the full history.
        let update = processor.flush_expired();
        Self::record_update_metrics(&update);
        update
    }

    fn process_gap(processor: &mut CandleProcessor, gap: &GapMarker) -> ProcessorUpdate {
        let update = processor.apply_gap(gap);
        if !update.closed.is_empty() || !update.incomplete_windows.is_empty() {
            info!(
                exchange = %gap.exchange,
                stream = %gap.stream,
                missing_from = gap.missing_from,
                missing_to = gap.missing_to,
                closed = update.closed.len(),
                incomplete_windows = update.incomplete_windows.len(),
                "recorded gap and closed candle windows at the event-time watermark"
            );
        }
        update
    }

    fn process_event(
        processor: &mut CandleProcessor,
        decoders: &mut HashMap<CexId, SharedTradeDecoder>,
        event: &MarketEvent,
    ) -> ProcessorUpdate {
        let decoder = match decoders.entry(event.exchange) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => match TradeDecoderFactory::for_exchange(event.exchange) {
                Ok(decoder) => entry.insert(decoder),
                Err(error) => {
                    MarketDataMetrics::record_dropped_event(
                        STAGE_AGGREGATOR,
                        DROP_REASON_NO_DECODER,
                    );
                    warn!(%error, exchange = %event.exchange, "no trade decoder for exchange");
                    return processor.apply_event(event, &[]);
                }
            },
        };
        match decoder.decode(event) {
            Ok(trades) => processor.apply_event(event, &trades),
            Err(error) => {
                // Undecodable exchange streams still advance the event-time
                // watermark and are deduplicated, but they contribute no trades.
                MarketDataMetrics::record_decode_failure(STAGE_AGGREGATOR, DECODE_KIND_TRADE);
                warn!(%error, stream = %event.stream, "skipping unreadable market event");
                processor.apply_event(event, &[])
            }
        }
    }

    #[cfg(test)]
    pub(super) fn closed_open_keys(
        update: &ProcessorUpdate,
        state: &crate::state::AggregatorState,
    ) -> Vec<platform_market_data::candle::CandleKey> {
        super::output::closed_open_keys(update, state)
    }
}

fn input_timestamp(input: &MarketInput) -> TimestampMilliseconds {
    match input {
        MarketInput::Event(event) => event.exchange_timestamp.unwrap_or(event.received_at),
        MarketInput::Gap(gap) => gap.detected_at,
    }
}
