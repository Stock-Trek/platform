//! Archive-backed keyed state rebuild for aggregator shards.
//!
//! When a compatible checkpoint is unavailable, shard state is rebuilt by
//! replaying the compacted event archive plus the raw tail that has not been
//! compacted yet.

use super::replay::{ArchivedEventSource, S3ArchivedEventSource};
use crate::processor::{CandleProcessor, ProcessorConfig};
use crate::shard::ShardId;
use crate::state::AggregatorState;
use platform_errors::BuildResult;
use platform_market_data::candle::AlignedWindow;
use platform_metrics::AggregatorMetrics;
use platform_metrics::descriptions::{DECODE_KIND_TRADE, DROP_REASON_NO_DECODER, STAGE_AGGREGATOR};
use platform_metrics::market_data::MarketDataMetrics;
use platform_types::new_types::DurationMilliseconds;
use std::collections::{BTreeMap, HashMap};
use tracing::warn;

pub(super) struct RebuiltArchiveState {
    pub(super) state: AggregatorState,
    pub(super) replayed_events: u64,
}

impl AggregatorState {
    /// Replays all archived events for one shard into a fresh keyed state.
    pub async fn rebuild_from_archive(
        archive: &mut dyn ArchivedEventSource,
        shard: ShardId,
        intervals: &[AlignedWindow],
        shard_count: usize,
        allowed_lateness: DurationMilliseconds,
        max_closed_candles_per_interval: BTreeMap<AlignedWindow, usize>,
        correction_cutoff: DurationMilliseconds,
    ) -> BuildResult<Self> {
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
}

impl RebuiltArchiveState {
    pub(super) async fn rebuild(
        archive: &mut dyn ArchivedEventSource,
        shard: ShardId,
        shard_count: usize,
        config: ProcessorConfig,
    ) -> BuildResult<Self> {
        let mut processor = CandleProcessor::with_config(config);
        let mut decoders = HashMap::new();
        let mut replayed = 0_u64;

        while let Some(archived) = archive.next_event().await? {
            replayed = replayed.saturating_add(1);
            if replayed.is_multiple_of(1_000) {
                AggregatorMetrics::record_replay_events(shard.get(), "archive", 1_000);
            }
            let event = &archived.event;
            let decoded = match S3ArchivedEventSource::decoder_for(&mut decoders, event.exchange) {
                Ok(decoder) => match decoder.decode(event) {
                    Ok(trades) => Some((
                        decoder,
                        ShardId::trades_for_shard(trades, shard, shard_count),
                    )),
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
            let owned_by_shard = match &decoded {
                Some((decoder, trades)) if trades.is_empty() => decoder
                    .trading_pair_for_stream(&event.stream)
                    .is_ok_and(|trading_pair| {
                        ShardId::for_trading_pair(event.exchange, trading_pair, shard_count)
                            == shard
                    }),
                _ => true,
            };
            // Closed candles produced during rebuild are already represented by
            // the archive's candle records; the fold reconstructs the same keyed
            // state, deduplication ranges, gaps, and event-time watermarks. Events
            // owned by another shard must not move this shard's watermark, while
            // non-trade events owned through their stream still advance event time.
            // Undecodable events are folded with no trades so archive recovery
            // matches live dispatch.
            if owned_by_shard {
                // Recovery continues the lineage that produced the archived inputs,
                // so the same processing step produces the same deterministic
                // revision ids. A new epoch is reserved for an explicit output
                // rebuild rather than ordinary state recovery.
                let state = processor.state_mut();
                state.state_epoch = state.state_epoch.max(archived.state_epoch);
                let trades = decoded
                    .as_ref()
                    .map_or(&[][..], |(_, trades)| trades.as_slice());
                let _ = processor.apply_event(event, trades);
            }
        }
        AggregatorMetrics::record_replay_events(shard.get(), "archive", replayed % 1_000);

        Ok(RebuiltArchiveState {
            state: processor.into_state(),
            replayed_events: replayed,
        })
    }
}
